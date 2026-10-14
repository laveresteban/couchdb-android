package org.couchdb.sync

import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.UUID

/** A change in the local database: the doc's latest local sequence number. */
data class LocalChange(
    val id: String,
    val seq: Long,
)

/**
 * Document store with a full revision tree per document, the same model
 * CouchDB uses, so local edits and replicated revisions merge the same way
 * they would on the server.
 *
 * Tables:
 * - `docs`: one row per document with the winning revision and the local
 *   sequence of its last change (drives push).
 * - `revs`: every known revision. Leaves have a body; ancestors fetched only
 *   as history have `body = NULL`.
 * - `local_docs`: non-replicated docs (checkpoints), like CouchDB `_local`.
 * - `meta`: store uuid and the sequence counter.
 * - `indexes` / `idx`: single-field indexes over winning revisions, used
 *   by [find] to narrow candidates before the full selector runs.
 */
class SqlLocalStore(
    private val db: SqlDriver,
) {
    private val lock = Any()

    /** Held by a [Replicator] while it syncs this store, so two syncs don't interleave. */
    val syncLock = Mutex()

    // Docs whose winner changed in the current transaction; reported after commit.
    private var depth = 0
    private val changedIds = LinkedHashSet<String>()

    init {
        synchronized(lock) {
            tx {
                db.execute(
                    """CREATE TABLE IF NOT EXISTS docs (
                        doc_id TEXT PRIMARY KEY, winning_rev TEXT NOT NULL,
                        deleted INTEGER NOT NULL, seq INTEGER NOT NULL)""",
                )
                db.execute("CREATE INDEX IF NOT EXISTS docs_seq ON docs(seq)")
                db.execute(
                    """CREATE TABLE IF NOT EXISTS revs (
                        doc_id TEXT NOT NULL, rev TEXT NOT NULL, parent TEXT,
                        deleted INTEGER NOT NULL, leaf INTEGER NOT NULL, body TEXT,
                        PRIMARY KEY (doc_id, rev))""",
                )
                db.execute("CREATE TABLE IF NOT EXISTS local_docs (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
                db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                db.execute("INSERT OR IGNORE INTO meta VALUES ('uuid', ?)", listOf(UUID.randomUUID().toString()))
                db.execute("INSERT OR IGNORE INTO meta VALUES ('seq', '0')")
                db.execute("CREATE TABLE IF NOT EXISTS indexes (field TEXT PRIMARY KEY)")
                // t is the collation rank (see Mango.rank); num/str hold the value.
                db.execute(
                    """CREATE TABLE IF NOT EXISTS idx (
                        field TEXT NOT NULL, doc_id TEXT NOT NULL, t INTEGER NOT NULL,
                        num REAL, str TEXT, PRIMARY KEY (field, doc_id))""",
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_num ON idx(field, t, num)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_str ON idx(field, t, str)")
                db.execute("CREATE TABLE IF NOT EXISTS cindexes (name TEXT PRIMARY KEY)")
                // key is the sortable encoding of the index fields (see sortKey).
                db.execute(
                    """CREATE TABLE IF NOT EXISTS cidx (
                        name TEXT NOT NULL, doc_id TEXT NOT NULL, key TEXT NOT NULL,
                        PRIMARY KEY (name, doc_id))""",
                )
                db.execute("CREATE INDEX IF NOT EXISTS cidx_key ON cidx(name, key)")
            }
        }
    }

    private var indexed: List<String> = synchronized(lock) { db.query("SELECT field FROM indexes") { it.string(0)!! } }
    private var compounds: List<List<String>> =
        synchronized(lock) {
            db.query("SELECT name FROM cindexes") { it.string(0)!!.split(FIELD_SEP) }
        }

    /** Closes the underlying database. Later calls fail. */
    fun close() = synchronized(lock) { db.close() }

    /**
     * Called with a doc id after its winning revision changes (local write or
     * replication), once the change is committed.
     */
    @Volatile var onChange: ((String) -> Unit)? = null

    /** Runs [block] atomically. Store calls inside it join the transaction. */
    fun <T> transaction(block: () -> T): T = synchronized(lock) { tx(block) }

    /** [SqlDriver.transaction] that reports changed docs to [onChange] after the outermost commit. */
    private fun <T> tx(block: () -> T): T {
        depth++
        val result =
            try {
                db.transaction(block)
            } catch (e: Throwable) {
                if (depth == 1) changedIds.clear()
                throw e
            } finally {
                depth--
            }
        if (depth == 0 && changedIds.isNotEmpty()) {
            val ids = changedIds.toList()
            changedIds.clear()
            ids.forEach { onChange?.invoke(it) }
        }
        return result
    }

    /** Stable id of this store, used to name replication checkpoints. */
    val uuid: String get() = synchronized(lock) { meta("uuid")!! }

    val lastSeq: Long get() = synchronized(lock) { meta("seq")!!.toLong() }

    /** The winning revision, or null if the doc doesn't exist or its winner is deleted. */
    fun get(id: String): Doc? =
        synchronized(lock) {
            val rev = winner(id) ?: return null
            if (rev.second) null else getRev(id, rev.first)
        }

    fun getRev(
        id: String,
        rev: String,
    ): Doc? =
        synchronized(lock) {
            db
                .query("SELECT deleted, body FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev)) {
                    val body = it.string(1) ?: return@query null
                    Doc(id, rev, Json.parseToJsonElement(body).jsonObject, it.long(0) == 1L)
                }.firstOrNull()
        }

    /**
     * Writes a new revision as a child of [prevRev]. [prevRev] must be a
     * current leaf, or null to create the doc (or recreate a deleted one).
     */
    fun put(
        id: String,
        body: JsonObject,
        prevRev: String?,
        deleted: Boolean = false,
    ): String =
        synchronized(lock) {
            tx {
                val current = winner(id)
                val parent: String? =
                    when {
                        prevRev != null -> {
                            if (!isLeaf(id, prevRev)) throw ConflictException("$id: $prevRev is not a current revision")
                            prevRev
                        }
                        current == null -> null
                        current.second -> current.first // extend the tombstone, as CouchDB does
                        else -> throw ConflictException("$id already exists")
                    }
                val clean = body.withoutMeta()
                val gen = (parent?.let { Rev.parse(it).gen } ?: 0) + 1
                val rev = "$gen-${md5Hex("$parent|$deleted|$clean")}"
                insertRev(id, rev, parent, deleted, clean)
                updateWinner(id)
                rev
            }
        }

    fun delete(
        id: String,
        rev: String,
    ): String = put(id, JsonObject(emptyMap()), rev, deleted = true)

    /**
     * Stores a revision received from another database. [history] is the
     * revision path newest first (from `_revisions`); missing ancestors are
     * added as body-less stubs. Returns false if the revision was already known.
     */
    fun insertRevision(
        id: String,
        history: List<String>,
        body: JsonObject,
        deleted: Boolean,
    ): Boolean =
        synchronized(lock) {
            require(history.isNotEmpty())
            tx {
                if (hasRev(id, history[0])) return@tx false
                // Add unknown ancestors oldest first so each one's parent exists.
                for (i in history.lastIndex downTo 1) {
                    val rev = history[i]
                    if (!hasRev(id, rev)) {
                        db.execute(
                            "INSERT INTO revs VALUES (?, ?, ?, 0, 0, NULL)",
                            listOf(id, rev, history.getOrNull(i + 1)),
                        )
                    }
                }
                // Every ancestor now has a child, so none of them can be a leaf anymore.
                for (ancestor in history.drop(1)) {
                    db.execute("UPDATE revs SET leaf = 0 WHERE doc_id = ? AND rev = ?", listOf(id, ancestor))
                }
                insertRev(id, history[0], history.getOrNull(1), deleted, body.withoutMeta())
                updateWinner(id)
                true
            }
        }

    fun leafRevs(id: String): List<String> =
        synchronized(lock) {
            db.query("SELECT rev FROM revs WHERE doc_id = ? AND leaf = 1", listOf(id)) { it.string(0)!! }
        }

    /** The winning revision, including a deletion. Null if the doc was never stored. */
    fun winningRev(id: String): String? = synchronized(lock) { winner(id)?.first }

    /** Non-winning, non-deleted leaves: the doc's conflicts. */
    fun conflicts(id: String): List<Doc> =
        synchronized(lock) {
            val win = winner(id)?.first ?: return emptyList()
            db
                .query(
                    "SELECT rev FROM revs WHERE doc_id = ? AND leaf = 1 AND deleted = 0 AND rev != ?",
                    listOf(id, win),
                ) { it.string(0)!! }
                .mapNotNull { getRev(id, it) }
        }

    /** Ids of docs that have conflicts. */
    fun conflictIds(): List<String> =
        synchronized(lock) {
            db.query(
                "SELECT DISTINCT r.doc_id FROM revs r JOIN docs d ON d.doc_id = r.doc_id " +
                    "WHERE r.leaf = 1 AND r.deleted = 0 AND r.rev != d.winning_rev",
            ) { it.string(0)!! }
        }

    /** How many documents changed after [seq]: what a push still has to send. */
    fun countChangesSince(seq: Long): Long =
        synchronized(lock) {
            db.query("SELECT COUNT(*) FROM docs WHERE seq > ?", listOf(seq)) { it.long(0)!! }.single()
        }

    /** Revision path from [rev] back to the oldest known ancestor, newest first. */
    fun revisionPath(
        id: String,
        rev: String,
    ): List<String> =
        synchronized(lock) {
            val path = mutableListOf<String>()
            var cur: String? = rev
            while (cur != null) {
                path += cur
                cur =
                    db
                        .query("SELECT parent FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, cur)) {
                            it.string(0)
                        }.firstOrNull()
            }
            path
        }

    /** Which of the given revisions this store doesn't have (like `_revs_diff`). */
    fun missingRevs(revs: Map<String, List<String>>): Map<String, List<String>> =
        synchronized(lock) {
            revs.mapValues { (id, list) -> list.filterNot { hasRev(id, it) } }.filterValues { it.isNotEmpty() }
        }

    fun changesSince(
        seq: Long,
        limit: Int,
    ): List<LocalChange> =
        synchronized(lock) {
            db.query("SELECT doc_id, seq FROM docs WHERE seq > ? ORDER BY seq LIMIT ?", listOf(seq, limit)) {
                LocalChange(it.string(0)!!, it.long(1)!!)
            }
        }

    fun allDocs(): List<Doc> =
        synchronized(lock) {
            db
                .query("SELECT doc_id FROM docs WHERE deleted = 0 ORDER BY doc_id") { it.string(0)!! }
                .mapNotNull { get(it) }
        }

    fun getLocal(id: String): JsonObject? =
        synchronized(lock) {
            db
                .query("SELECT body FROM local_docs WHERE id = ?", listOf(id)) {
                    Json.parseToJsonElement(it.string(0)!!).jsonObject
                }.firstOrNull()
        }

    fun putLocal(
        id: String,
        body: JsonObject,
    ) = synchronized(lock) {
        db.execute("INSERT OR REPLACE INTO local_docs VALUES (?, ?)", listOf(id, body.toString()))
    }

    private fun insertRev(
        id: String,
        rev: String,
        parent: String?,
        deleted: Boolean,
        body: JsonObject,
    ) {
        if (parent != null) db.execute("UPDATE revs SET leaf = 0 WHERE doc_id = ? AND rev = ?", listOf(id, parent))
        db.execute(
            "INSERT INTO revs VALUES (?, ?, ?, ?, 1, ?)",
            listOf(id, rev, parent, if (deleted) 1 else 0, body.toString()),
        )
    }

    /**
     * Picks the winner the way CouchDB does: non-deleted leaves beat deleted
     * ones, then the higher revision (generation, then hash) wins. Every
     * replica picks the same winner without talking to the others.
     */
    private fun updateWinner(id: String) {
        val leaves =
            db.query("SELECT rev, deleted FROM revs WHERE doc_id = ? AND leaf = 1", listOf(id)) {
                it.string(0)!! to (it.long(1) == 1L)
            }
        val win = leaves.maxWith(compareBy<Pair<String, Boolean>> { !it.second }.thenBy { Rev.parse(it.first) })
        val seq = meta("seq")!!.toLong() + 1
        db.execute("UPDATE meta SET value = ? WHERE key = 'seq'", listOf(seq.toString()))
        db.execute(
            "INSERT OR REPLACE INTO docs VALUES (?, ?, ?, ?)",
            listOf(id, win.first, if (win.second) 1 else 0, seq),
        )
        reindex(id, if (win.second) null else bodyOf(id, win.first))
        changedIds += id
    }

    private fun bodyOf(
        id: String,
        rev: String,
    ): JsonObject? =
        db
            .query("SELECT body FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev)) { it.string(0) }
            .firstOrNull()
            ?.let { Json.parseToJsonElement(it).jsonObject }

    private fun reindex(
        id: String,
        body: JsonObject?,
    ) {
        if (indexed.isNotEmpty()) {
            db.execute("DELETE FROM idx WHERE doc_id = ?", listOf(id))
            if (body != null) {
                for (f in indexed) {
                    val v = Mango.field(body, f) ?: continue
                    db.execute("INSERT INTO idx VALUES (?, ?, ?, ?, ?)", listOf(f, id, Mango.rank(v), numOf(v), strOf(v)))
                }
            }
        }
        if (compounds.isNotEmpty()) {
            db.execute("DELETE FROM cidx WHERE doc_id = ?", listOf(id))
            if (body != null) {
                for (fields in compounds) {
                    db.execute("INSERT INTO cidx VALUES (?, ?, ?)", listOf(indexName(fields), id, sortKey(body, fields)))
                }
            }
        }
    }

    private fun winner(id: String): Pair<String, Boolean>? =
        db
            .query("SELECT winning_rev, deleted FROM docs WHERE doc_id = ?", listOf(id)) {
                it.string(0)!! to (it.long(1) == 1L)
            }.firstOrNull()

    private fun hasRev(
        id: String,
        rev: String,
    ) = db.query("SELECT 1 FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev)) { 1 }.isNotEmpty()

    private fun isLeaf(
        id: String,
        rev: String,
    ) = db.query("SELECT 1 FROM revs WHERE doc_id = ? AND rev = ? AND leaf = 1", listOf(id, rev)) { 1 }.isNotEmpty()

    private fun meta(key: String) = db.query("SELECT value FROM meta WHERE key = ?", listOf(key)) { it.string(0) }.firstOrNull()

    /** Adds a single-field index (dotted paths allowed) and fills it from existing docs. */
    fun createIndex(field: String) =
        synchronized(lock) {
            db.transaction {
                if (field in indexed) return@transaction
                db.execute("INSERT INTO indexes VALUES (?)", listOf(field))
                indexed = indexed + field
                db
                    .query("SELECT doc_id, winning_rev FROM docs WHERE deleted = 0") { it.string(0)!! to it.string(1)!! }
                    .forEach { (id, rev) ->
                        val v = bodyOf(id, rev)?.let { Mango.field(it, field) } ?: return@forEach
                        db.execute("INSERT INTO idx VALUES (?, ?, ?, ?, ?)", listOf(field, id, Mango.rank(v), numOf(v), strOf(v)))
                    }
            }
        }

    /**
     * Adds an index over several fields, filled from existing docs. A sort
     * on a prefix of [fields], all in one direction, reads the index instead
     * of sorting in memory. Ties on that prefix come back in full-index order,
     * not by id.
     */
    fun createCompoundIndex(fields: List<String>) =
        synchronized(lock) {
            require(fields.isNotEmpty() && fields.none { FIELD_SEP in it }) { "bad index fields: $fields" }
            db.transaction {
                if (fields in compounds) return@transaction
                db.execute("INSERT INTO cindexes VALUES (?)", listOf(indexName(fields)))
                compounds = compounds + listOf(fields)
                db
                    .query("SELECT doc_id, winning_rev FROM docs WHERE deleted = 0") { it.string(0)!! to it.string(1)!! }
                    .forEach { (id, rev) ->
                        val body = bodyOf(id, rev) ?: return@forEach
                        db.execute("INSERT INTO cidx VALUES (?, ?, ?)", listOf(indexName(fields), id, sortKey(body, fields)))
                    }
            }
        }

    val indexes: List<String> get() = indexed

    /**
     * Runs a Mango query over winning revisions. If the sort matches a
     * compound index, documents come back in index order. Otherwise, if the
     * selector has a top-level condition on an indexed field, only matching
     * index rows are loaded. The full selector is always checked.
     */
    fun find(query: Query): List<Doc> =
        synchronized(lock) {
            val ordered = ordered(query.sort)
            val ids =
                ordered ?: candidates(query.selector)
                    ?: db.query("SELECT doc_id FROM docs WHERE deleted = 0") { it.string(0)!! }
            var docs = ids.mapNotNull { get(it) }.filter { Mango.matches(it.body, query.selector) }
            if (ordered == null && query.sort.isNotEmpty()) {
                docs =
                    docs.sortedWith { a, b ->
                        for ((f, asc) in query.sort) {
                            val x = Mango.field(a.body, f)
                            val y = Mango.field(b.body, f)
                            val c =
                                when {
                                    x == null && y == null -> 0
                                    x == null -> -1
                                    y == null -> 1
                                    else -> Mango.compare(x, y)
                                }
                            if (c != 0) return@sortedWith if (asc) c else -c
                        }
                        a.id.compareTo(b.id)
                    }
            } else if (ordered == null) {
                docs = docs.sortedBy { it.id }
            }
            docs.drop(query.skip).let { d -> query.limit?.let { d.take(it) } ?: d }
        }

    /** Doc ids in the order of a compound index that matches [sort], or null if none does. */
    private fun ordered(sort: List<Pair<String, Boolean>>): List<String>? {
        if (sort.isEmpty()) return null
        val asc = sort.first().second
        if (sort.any { it.second != asc }) return null
        val fields = sort.map { it.first }
        val index = compounds.firstOrNull { it.size >= fields.size && it.subList(0, fields.size) == fields } ?: return null
        return db.query(
            "SELECT doc_id FROM cidx WHERE name = ? ORDER BY key ${if (asc) "ASC" else "DESC"}, doc_id ASC",
            listOf(indexName(index)),
        ) { it.string(0)!! }
    }

    private fun indexName(fields: List<String>) = fields.joinToString(FIELD_SEP)

    /** Doc ids from an index, or null if no indexed condition applies. */
    private fun candidates(selector: JsonObject): List<String>? {
        for ((field, cond) in selector) {
            if (field !in indexed) continue
            val ops: Map<String, JsonElement> =
                if (cond is JsonObject && cond.keys.all { it.startsWith("$") }) {
                    cond
                } else if (cond is JsonObject) {
                    continue
                } else {
                    mapOf("\$eq" to cond)
                }
            val where = mutableListOf("field = ?")
            val args = mutableListOf<Any?>(field)
            for ((op, v) in ops) {
                val col =
                    when {
                        v is JsonPrimitive && !v.isString && v.booleanOrNull == null && v !is kotlinx.serialization.json.JsonNull -> "num"
                        v is JsonPrimitive && v.isString -> "str"
                        else -> null
                    }
                if (op == "\$in" && v is kotlinx.serialization.json.JsonArray) {
                    // Arrays in the doc can match $in too, so also keep array-valued rows.
                    val vals = v.jsonArray
                    if (vals.any { it !is JsonPrimitive || (!it.isString && it.doubleOrNull == null) }) continue
                    val (nums, strs) = vals.partition { !it.jsonPrimitive.isString }
                    val parts = mutableListOf("t = 5")
                    if (nums.isNotEmpty()) {
                        parts += "(t = 3 AND num IN (${nums.joinToString { "?" }}))"
                        args.addAll(nums.map { it.jsonPrimitive.doubleOrNull })
                    }
                    if (strs.isNotEmpty()) {
                        parts += "(t = 4 AND str IN (${strs.joinToString { "?" }}))"
                        args.addAll(strs.map { it.jsonPrimitive.content })
                    }
                    where += "(${parts.joinToString(" OR ")})"
                    continue
                }
                if (col == null) continue
                val sql =
                    when (op) {
                        "\$eq" -> "="
                        "\$gt" -> ">"
                        "\$gte" -> ">="
                        "\$lt" -> "<"
                        "\$lte" -> "<="
                        else -> null
                    } ?: continue
                // Ranges cross types in CouchDB collation (null < bool < number < string < array < object).
                val t = if (col == "num") 3 else 4
                where +=
                    when (sql) {
                        "=" -> "t = $t AND $col = ?"
                        ">", ">=" -> "((t = $t AND $col $sql ?) OR t > $t)"
                        else -> "((t = $t AND $col $sql ?) OR t < $t)"
                    }
                args += if (col == "num") v.jsonPrimitive.doubleOrNull else v.jsonPrimitive.content
            }
            if (where.size == 1) continue
            return db.query("SELECT doc_id FROM idx WHERE ${where.joinToString(" AND ")}", args) { it.string(0)!! }
        }
        return null
    }

    /**
     * Frees space the way CouchDB compaction does: drops bodies of non-leaf
     * revisions and forgets ancestors more than [revsLimit] generations
     * behind the newest leaf. Returns the attachment digests still in use, so
     * the caller can delete unreferenced blobs.
     */
    fun compact(revsLimit: Int = 1000): Set<String> =
        synchronized(lock) {
            db.transaction {
                db.execute("UPDATE revs SET body = NULL WHERE leaf = 0")
                val rows = db.query("SELECT doc_id, rev, leaf FROM revs") { Triple(it.string(0)!!, it.string(1)!!, it.long(2) == 1L) }
                for ((id, revs) in rows.groupBy { it.first }) {
                    val newest = revs.filter { it.third }.maxOf { Rev.parse(it.second).gen }
                    for ((_, rev, leaf) in revs) {
                        if (!leaf && Rev.parse(rev).gen <= newest - revsLimit) {
                            db.execute("DELETE FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev))
                        }
                    }
                }
                db
                    .query("SELECT body FROM revs WHERE body IS NOT NULL") { it.string(0)!! }
                    .flatMap { body ->
                        (Json.parseToJsonElement(body).jsonObject["_attachments"] as? JsonObject).orEmpty().values.mapNotNull {
                            (it as? JsonObject)?.get("digest")?.jsonPrimitive?.content
                        }
                    }.toSet()
            }
        }
}

private const val FIELD_SEP = "\u001f"

private fun numOf(v: JsonElement) = if (Mango.rank(v) == 3) v.jsonPrimitive.doubleOrNull else null

private fun strOf(v: JsonElement) = if (Mango.rank(v) == 4) v.jsonPrimitive.content else null

/**
 * Sort key for a compound index: one segment per field, whose plain string
 * order matches [Mango.compare] (missing < null < false < true < numbers <
 * strings < arrays < objects). Arrays and objects sort by their JSON text.
 */
internal fun sortKey(
    body: JsonObject,
    fields: List<String>,
): String =
    fields.joinToString("") { f ->
        val v = Mango.field(body, f) ?: return@joinToString "-!"
        val rank = Mango.rank(v)
        val payload =
            when (rank) {
                3 -> numberKey(v.jsonPrimitive.doubleOrNull!!)
                4 ->
                    v.jsonPrimitive.content
                        .map { "%04x".format(it.code) }
                        .joinToString("")
                5, 6 -> v.toString().map { "%04x".format(it.code) }.joinToString("")
                else -> ""
            }
        "$rank$payload!"
    }

/** Big-endian hex of a double's bits, flipped so the strings sort like the numbers. */
private fun numberKey(d: Double): String {
    val bits = java.lang.Double.doubleToLongBits(d)
    return "%016x".format(if (bits < 0) bits.inv() else bits or Long.MIN_VALUE)
}
