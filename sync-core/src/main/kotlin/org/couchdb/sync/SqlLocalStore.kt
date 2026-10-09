package org.couchdb.sync

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import java.util.UUID

/** A change in the local database: the doc's latest local sequence number. */
data class LocalChange(val id: String, val seq: Long)

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
 */
class SqlLocalStore(private val db: SqlDriver) {
    private val lock = Any()

    init {
        synchronized(lock) {
            db.transaction {
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
            }
        }
    }

    /** Stable id of this store, used to name replication checkpoints. */
    val uuid: String get() = synchronized(lock) { meta("uuid")!! }

    val lastSeq: Long get() = synchronized(lock) { meta("seq")!!.toLong() }

    /** The winning revision, or null if the doc doesn't exist or its winner is deleted. */
    fun get(id: String): Doc? = synchronized(lock) {
        val rev = winner(id) ?: return null
        if (rev.second) null else getRev(id, rev.first)
    }

    fun getRev(id: String, rev: String): Doc? = synchronized(lock) {
        db.query("SELECT deleted, body FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev)) {
            val body = it.string(1) ?: return@query null
            Doc(id, rev, Json.parseToJsonElement(body).jsonObject, it.long(0) == 1L)
        }.firstOrNull()
    }

    /**
     * Writes a new revision as a child of [prevRev]. [prevRev] must be a
     * current leaf, or null to create the doc (or recreate a deleted one).
     */
    fun put(id: String, body: JsonObject, prevRev: String?, deleted: Boolean = false): String =
        synchronized(lock) {
            db.transaction {
                val current = winner(id)
                val parent: String? = when {
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

    fun delete(id: String, rev: String): String = put(id, JsonObject(emptyMap()), rev, deleted = true)

    /**
     * Stores a revision received from another database. [history] is the
     * revision path newest first (from `_revisions`); missing ancestors are
     * added as body-less stubs. Returns false if the revision was already known.
     */
    fun insertRevision(id: String, history: List<String>, body: JsonObject, deleted: Boolean): Boolean =
        synchronized(lock) {
            require(history.isNotEmpty())
            db.transaction {
                if (hasRev(id, history[0])) return@transaction false
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
                insertRev(id, history[0], history.getOrNull(1), deleted, body.withoutMeta())
                updateWinner(id)
                true
            }
        }

    fun leafRevs(id: String): List<String> = synchronized(lock) {
        db.query("SELECT rev FROM revs WHERE doc_id = ? AND leaf = 1", listOf(id)) { it.string(0)!! }
    }

    /** Non-winning, non-deleted leaves: the doc's conflicts. */
    fun conflicts(id: String): List<Doc> = synchronized(lock) {
        val win = winner(id)?.first ?: return emptyList()
        db.query(
            "SELECT rev FROM revs WHERE doc_id = ? AND leaf = 1 AND deleted = 0 AND rev != ?",
            listOf(id, win),
        ) { it.string(0)!! }.mapNotNull { getRev(id, it) }
    }

    /** Revision path from [rev] back to the oldest known ancestor, newest first. */
    fun revisionPath(id: String, rev: String): List<String> = synchronized(lock) {
        val path = mutableListOf<String>()
        var cur: String? = rev
        while (cur != null) {
            path += cur
            cur = db.query("SELECT parent FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, cur)) {
                it.string(0)
            }.firstOrNull()
        }
        path
    }

    /** Which of the given revisions this store doesn't have (like `_revs_diff`). */
    fun missingRevs(revs: Map<String, List<String>>): Map<String, List<String>> = synchronized(lock) {
        revs.mapValues { (id, list) -> list.filterNot { hasRev(id, it) } }.filterValues { it.isNotEmpty() }
    }

    fun changesSince(seq: Long, limit: Int): List<LocalChange> = synchronized(lock) {
        db.query("SELECT doc_id, seq FROM docs WHERE seq > ? ORDER BY seq LIMIT ?", listOf(seq, limit)) {
            LocalChange(it.string(0)!!, it.long(1)!!)
        }
    }

    fun allDocs(): List<Doc> = synchronized(lock) {
        db.query("SELECT doc_id FROM docs WHERE deleted = 0 ORDER BY doc_id") { it.string(0)!! }
            .mapNotNull { get(it) }
    }

    fun getLocal(id: String): JsonObject? = synchronized(lock) {
        db.query("SELECT body FROM local_docs WHERE id = ?", listOf(id)) {
            Json.parseToJsonElement(it.string(0)!!).jsonObject
        }.firstOrNull()
    }

    fun putLocal(id: String, body: JsonObject) = synchronized(lock) {
        db.execute("INSERT OR REPLACE INTO local_docs VALUES (?, ?)", listOf(id, body.toString()))
    }

    private fun insertRev(id: String, rev: String, parent: String?, deleted: Boolean, body: JsonObject) {
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
        val leaves = db.query("SELECT rev, deleted FROM revs WHERE doc_id = ? AND leaf = 1", listOf(id)) {
            it.string(0)!! to (it.long(1) == 1L)
        }
        val win = leaves.maxWith(compareBy<Pair<String, Boolean>> { !it.second }.thenBy { Rev.parse(it.first) })
        val seq = meta("seq")!!.toLong() + 1
        db.execute("UPDATE meta SET value = ? WHERE key = 'seq'", listOf(seq.toString()))
        db.execute(
            "INSERT OR REPLACE INTO docs VALUES (?, ?, ?, ?)",
            listOf(id, win.first, if (win.second) 1 else 0, seq),
        )
    }

    private fun winner(id: String): Pair<String, Boolean>? =
        db.query("SELECT winning_rev, deleted FROM docs WHERE doc_id = ?", listOf(id)) {
            it.string(0)!! to (it.long(1) == 1L)
        }.firstOrNull()

    private fun hasRev(id: String, rev: String) =
        db.query("SELECT 1 FROM revs WHERE doc_id = ? AND rev = ?", listOf(id, rev)) { 1 }.isNotEmpty()

    private fun isLeaf(id: String, rev: String) =
        db.query("SELECT 1 FROM revs WHERE doc_id = ? AND rev = ? AND leaf = 1", listOf(id, rev)) { 1 }.isNotEmpty()

    private fun meta(key: String) =
        db.query("SELECT value FROM meta WHERE key = ?", listOf(key)) { it.string(0) }.firstOrNull()
}
