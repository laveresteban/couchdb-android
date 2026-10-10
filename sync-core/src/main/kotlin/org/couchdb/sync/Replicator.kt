package org.couchdb.sync

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.io.IOException
import java.util.Base64
import java.util.UUID

sealed interface SyncState {
    data object Idle : SyncState
    data class Pulling(val docs: Int) : SyncState
    data class Pushing(val docs: Int) : SyncState
    data class Error(val error: Throwable) : SyncState
}

/**
 * How many revisions moved each way. [pushFailures] are local revisions the
 * server refused (e.g. `forbidden` from `validate_doc_update`) or that couldn't
 * be sent; [pullErrors] are remote revisions `_bulk_get` couldn't return.
 * Neither stops sync: like CouchDB's replicator, they're reported and skipped.
 */
data class SyncResult(
    val pulled: Int,
    val pushed: Int,
    val pushFailures: List<WriteFailure> = emptyList(),
    val pullErrors: Int = 0,
)

/**
 * Two-way replication between a [SqlLocalStore] and a CouchDB database,
 * following the CouchDB replication protocol: read changes, diff revisions,
 * copy only the missing ones with their history, then checkpoint.
 *
 * Checkpoints are saved on both sides (local `local_docs` and remote
 * `_local/<id>`) with a shared session id. If the two disagree, for example
 * after the server database was recreated, replication starts from 0; the
 * revision diff makes that safe, just slower.
 *
 * Attachments: pull asks `_bulk_get` for content newer than the revisions
 * we already have (`atts_since`) and stores it in [blobs]; push sends
 * content inline unless the server already has it in an ancestor revision
 * (from `possible_ancestors`), in which case it sends a stub.
 *
 * With a [filter], pull uses the server's `_doc_ids` / `_selector` changes
 * filter and push applies the same filter to local changes. Deletions always
 * pass a selector filter, since a tombstone has no fields to match.
 *
 * Replications of the same store take turns (see [SqlLocalStore.syncLock]),
 * so background sync and live sync can't race on checkpoints. Pushes are
 * split so no `_bulk_docs` body exceeds [maxBatchBytes].
 */
class Replicator(
    private val store: SqlLocalStore,
    private val remote: RemoteDb,
    private val blobs: BlobStore = MemoryBlobStore(),
    private val filter: SyncFilter? = null,
    private val batchSize: Int = 100,
    private val io: CoroutineDispatcher = Dispatchers.IO,
    private val maxBatchBytes: Int = 4 * 1024 * 1024,
) {
    private val _state = MutableStateFlow<SyncState>(SyncState.Idle)
    val state: StateFlow<SyncState> = _state.asStateFlow()

    private val pullId = md5Hex("${store.uuid}|${remote.url}|pull|$filter")
    private val pushId = md5Hex("${store.uuid}|${remote.url}|push|$filter")

    /** Remote `_local` checkpoint revs from our last write, to skip a GET before each PUT. */
    private val checkpointRevs = mutableMapOf<String, String>()

    /** One pull then one push. Returns how many revisions moved each way. */
    suspend fun sync(createRemote: Boolean = false): SyncResult = store.syncLock.withLock {
        remote.ensureExists(createRemote)
        try {
            val (pulled, pullErrors) = pullCounting()
            val (pushed, failures) = pushCollecting()
            _state.value = SyncState.Idle
            SyncResult(pulled, pushed, failures, pullErrors)
        } catch (e: Exception) {
            _state.value = SyncState.Error(e)
            throw e
        }
    }

    /** Copies remote revisions this store is missing. Returns how many were stored. */
    suspend fun pull(): Int = pullCounting().first

    private suspend fun pullCounting(): Pair<Int, Int> {
        val cp = readCheckpoint(pullId)
        var since = cp.seq ?: "0"
        var total = 0
        var errors = 0
        while (true) {
            val page = remote.changes(since, batchSize, filter = filter)
            val requests = io {
                store.missingRevs(page.results.associate { it.id to it.revs }).flatMap { (id, revs) ->
                    val have = store.leafRevs(id)
                    revs.map { RevRequest(id, it, have) }
                }
            }
            val fetched = remote.bulkGet(requests, attachments = true)
            io {
                for (f in fetched) {
                    val doc = f.doc?.let(::storeAttachments) ?: continue
                    store.insertRevision(f.id, history(doc), doc, doc["_deleted"]?.jsonPrimitive?.boolean == true)
                }
            }
            total += fetched.count { it.doc != null }
            errors += fetched.count { it.doc == null }
            _state.value = SyncState.Pulling(total)
            if (page.results.isEmpty()) break
            since = page.lastSeq
            writeCheckpoint(pullId, cp.copy(seq = since))
            if (page.results.size < batchSize) break
        }
        return total to errors
    }

    /** Sends local revisions the remote is missing. Returns how many were stored. */
    suspend fun push(): Int = pushCollecting().first

    private suspend fun pushCollecting(): Pair<Int, List<WriteFailure>> {
        val cp = readCheckpoint(pushId)
        var since = cp.seq?.toLong() ?: 0L
        var total = 0
        val failures = mutableListOf<WriteFailure>()
        while (true) {
            val changes = io { store.changesSince(since, batchSize) }
            if (changes.isEmpty()) break
            val leaves = io { changes.filter { passesFilter(it.id) }.associate { it.id to store.leafRevs(it.id) } }
            val missing = remote.revsDiff(leaves)
            val docs = io {
                missing.flatMap { (id, diff) ->
                    diff.missing.mapNotNull { rev ->
                        try {
                            toWireDoc(id, rev, diff.possibleAncestors)
                        } catch (e: MissingAttachmentException) {
                            failures += WriteFailure(id, rev, "missing_attachment", e.message)
                            null
                        }
                    }
                }
            }
            for (chunk in bySize(docs)) {
                val refused = remote.bulkDocsNoNewEdits(chunk)
                failures += refused
                total += chunk.size - refused.size
            }
            _state.value = SyncState.Pushing(total)
            since = changes.last().seq
            writeCheckpoint(pushId, cp.copy(seq = since.toString()))
        }
        return total to failures
    }

    /** Splits [docs] so each `_bulk_docs` body stays under [maxBatchBytes] (a lone bigger doc goes alone). */
    private fun bySize(docs: List<JsonObject>): List<List<JsonObject>> {
        val chunks = mutableListOf<MutableList<JsonObject>>()
        var size = 0
        for (doc in docs) {
            val n = doc.toString().length
            if (chunks.isEmpty() || size + n > maxBatchBytes) {
                chunks += mutableListOf<JsonObject>()
                size = 0
            }
            chunks.last() += doc
            size += n
        }
        return chunks
    }

    /**
     * Keeps syncing until cancelled: waits for a remote change (longpoll) or
     * a local write, then syncs again. Network errors, 429 and 5xx back off
     * exponentially up to [maxBackoffMs]; other errors (auth, 4xx) stop the loop.
     */
    suspend fun live(
        localWrites: Flow<Unit>,
        longpollMs: Long = 60_000,
        debounceMs: Long = 2_000,
        maxBackoffMs: Long = 5 * 60_000,
    ) {
        var backoff = 1_000L
        while (currentCoroutineContext().isActive) {
            try {
                sync()
                backoff = 1_000L
                val since = readCheckpoint(pullId).seq ?: "0"
                coroutineScope {
                    val remoteChange = async { remote.changes(since, 1, longpollMs, filter) }
                    val localWrite = async { localWrites.first(); delay(debounceMs) }
                    select {
                        remoteChange.onAwait {}
                        localWrite.onAwait {}
                    }
                    remoteChange.cancel()
                    localWrite.cancel()
                }
            } catch (e: IOException) {
                _state.value = SyncState.Error(e)
                if (!ChangesFeed.isTransient(e)) throw e
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(maxBackoffMs)
            }
        }
    }

    private fun passesFilter(id: String): Boolean = when (val f = filter) {
        null -> true
        is SyncFilter.DocIds -> id in f.ids
        is SyncFilter.Selector -> store.get(id)?.let { Mango.matches(it.body, f.selector) } ?: true
    }

    /** Moves inline attachment data into [blobs] and leaves stubs in the body. */
    private fun storeAttachments(doc: JsonObject): JsonObject {
        val atts = doc["_attachments"] as? JsonObject ?: return doc
        val stubs = atts.mapValues { (_, a) ->
            val att = a.jsonObject
            val data = att["data"]?.jsonPrimitive?.content ?: return@mapValues att
            val bytes = Base64.getDecoder().decode(data)
            val digest = att["digest"]?.jsonPrimitive?.content ?: attachmentDigest(bytes)
            blobs.put(digest, bytes)
            JsonObject(
                att.filterKeys { it !in setOf("data", "encoding", "encoded_length", "follows") } + mapOf(
                    "digest" to JsonPrimitive(digest),
                    "length" to JsonPrimitive(bytes.size),
                    "stub" to JsonPrimitive(true),
                ),
            )
        }
        return JsonObject(doc + ("_attachments" to JsonObject(stubs)))
    }

    /**
     * Builds the wire form of a local revision: body plus `_id`, `_rev`,
     * `_deleted`, `_revisions`. Attachments the server has in one of
     * [serverAncestors] go as stubs, the rest inline.
     */
    private fun toWireDoc(id: String, rev: String, serverAncestors: List<String>): JsonObject? {
        val doc = store.getRev(id, rev) ?: return null
        val path = store.revisionPath(id, rev).map(Rev::parse)
        val known = serverAncestors.map(Rev::parse).filter { it in path }.maxOfOrNull { it.gen } ?: 0
        val atts = (doc.body["_attachments"] as? JsonObject)?.mapValues { (_, a) ->
            val att = a.jsonObject
            val revpos = att["revpos"]?.jsonPrimitive?.int ?: Int.MAX_VALUE
            val digest = att["digest"]?.jsonPrimitive?.content
            if (digest == null || revpos <= known) return@mapValues att // server has it: send the stub
            val bytes = blobs.get(digest)
                ?: throw MissingAttachmentException("$id $rev: attachment content for $digest isn't stored locally")
            JsonObject(
                att.filterKeys { it !in setOf("stub", "length", "digest") } +
                    ("data" to JsonPrimitive(Base64.getEncoder().encodeToString(bytes))),
            )
        }
        val body = if (atts == null) doc.body else JsonObject(doc.body + ("_attachments" to JsonObject(atts)))
        return JsonObject(
            body + buildJsonObject {
                put("_id", id)
                put("_rev", rev)
                if (doc.deleted) put("_deleted", true)
                put(
                    "_revisions",
                    buildJsonObject {
                        put("start", path.first().gen)
                        put("ids", JsonArray(path.map { JsonPrimitive(it.hash) }))
                    },
                )
            },
        )
    }

    private data class Checkpoint(val session: String, val seq: String?)

    private suspend fun readCheckpoint(id: String): Checkpoint {
        val local = io { store.getLocal(id) }
        val remoteDoc = remote.getLocal(id)
        val session = local?.get("session_id")?.jsonPrimitive?.contentOrNull
        return if (session != null && session == remoteDoc?.get("session_id")?.jsonPrimitive?.contentOrNull) {
            Checkpoint(session, local["last_seq"]?.jsonPrimitive?.contentOrNull)
        } else {
            Checkpoint(UUID.randomUUID().toString(), null)
        }
    }

    private suspend fun writeCheckpoint(id: String, cp: Checkpoint) {
        val body = buildJsonObject {
            put("session_id", cp.session)
            put("last_seq", cp.seq)
        }
        checkpointRevs[id] = putLocalWithRev(remote, id, body, checkpointRevs[id])
        io { store.putLocal(id, body) }
    }

    private suspend fun <T> io(block: () -> T): T = withContext(io) { block() }
}

/** Full revision ids from a doc's `_revisions`, newest first. */
internal fun history(doc: JsonObject): List<String> {
    val revs = doc["_revisions"]?.jsonObject ?: return listOf(doc["_rev"]!!.jsonPrimitive.content)
    val start = revs["start"]!!.jsonPrimitive.int
    return revs["ids"]!!.jsonArray.mapIndexed { i, h -> "${start - i}-${h.jsonPrimitive.content}" }
}

private class MissingAttachmentException(message: String) : RuntimeException(message)

/**
 * Writes `_local/<id>` using [knownRev] (our last write) as its `_rev`. If
 * that's stale or unknown, reads the current rev and tries once more.
 * Returns the new rev.
 */
internal suspend fun putLocalWithRev(remote: RemoteDb, id: String, body: JsonObject, knownRev: String?): String {
    fun withRev(rev: String?) = JsonObject(body + (rev?.let { mapOf("_rev" to JsonPrimitive(it)) } ?: emptyMap()))
    if (knownRev != null) {
        try {
            return remote.putLocal(id, withRev(knownRev))
        } catch (e: CouchException) {
            if (e.status != 409) throw e
        }
    }
    val current = remote.getLocal(id)?.get("_rev")?.jsonPrimitive?.contentOrNull
    return remote.putLocal(id, withRev(current))
}
