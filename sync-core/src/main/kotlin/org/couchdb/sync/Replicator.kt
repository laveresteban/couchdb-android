package org.couchdb.sync

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
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
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.IOException
import java.util.Base64
import java.util.UUID

sealed interface SyncState {
    data object Idle : SyncState

    /** [docs] revisions pulled so far; [pending] changes the server has left after this batch, if it says. */
    data class Pulling(val docs: Int, val pending: Long?) : SyncState

    /** [docs] revisions pushed so far; [pending] local changes not yet pushed. */
    data class Pushing(val docs: Int, val pending: Long) : SyncState

    data class Error(val error: Throwable) : SyncState
}

/**
 * How many revisions moved each way. [pushFailures] are local revisions the
 * server refused (e.g. `forbidden` from `validate_doc_update`) or that couldn't
 * be sent; [pullErrors] are remote revisions that couldn't be fetched, or
 * whose attachment content failed its digest check. Neither stops sync: like
 * CouchDB's replicator, they're reported and skipped.
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
 * Attachments: pull fetches each missing attachment's content on its own
 * (streamed into [blobs]) and skips content already stored. Push sends small
 * content inline in `_bulk_docs` and streams content over [inlineAttachmentBytes]
 * as a `multipart/related` part. Content the server already has in an ancestor
 * revision goes as a stub.
 *
 * With a [filter], pull uses the server's `_doc_ids` / `_selector` changes
 * filter and push applies the same filter to local changes. Deletions always
 * pass a selector filter, since a tombstone has no fields to match.
 *
 * With a [resolver], conflicts from each pull are settled right away, before
 * the push that follows.
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
    private val inlineAttachmentBytes: Long = 1024 * 1024,
    private val resolver: ConflictResolver? = null,
) {
    private val _state = MutableStateFlow<SyncState>(SyncState.Idle)
    val state: StateFlow<SyncState> = _state.asStateFlow()

    private val pullId = md5Hex("${store.uuid}|${remote.url}|pull|$filter")
    private val pushId = md5Hex("${store.uuid}|${remote.url}|push|$filter")

    /** Remote `_local` checkpoint revs from our last write, to skip a GET before each PUT. */
    private val checkpointRevs = mutableMapOf<String, String>()

    /** One pull then one push. Returns how many revisions moved each way. */
    suspend fun sync(createRemote: Boolean = false): SyncResult =
        store.syncLock.withLock {
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
        val touched = LinkedHashSet<String>()
        while (true) {
            val page = remote.changes(since, batchSize, filter = filter)
            val requests =
                io {
                    store.missingRevs(page.results.associate { it.id to it.revs })
                        .flatMap { (id, revs) -> revs.map { RevRequest(id, it) } }
                }
            for (f in remote.bulkGet(requests)) {
                val doc = f.doc
                if (doc == null || !downloadAttachments(f.id, doc)) {
                    errors++
                    continue
                }
                val deleted = doc["_deleted"]?.jsonPrimitive?.boolean == true
                if (io { store.insertRevision(f.id, history(doc), doc, deleted) }) touched += f.id
                total++
            }
            _state.value = SyncState.Pulling(total, page.pending)
            if (page.results.isEmpty()) break
            since = page.lastSeq
            writeCheckpoint(pullId, cp.copy(seq = since))
            if (page.results.size < batchSize) break
        }
        resolver?.let { r -> touched.forEach { resolveConflict(store, io, it, r) } }
        return total to errors
    }

    /** Fetches attachment content this store doesn't have yet. Returns false if some of it fails its digest check. */
    private suspend fun downloadAttachments(
        id: String,
        doc: JsonObject,
    ): Boolean {
        val rev = doc["_rev"]!!.jsonPrimitive.content
        val atts = doc["_attachments"] as? JsonObject ?: return true
        for ((name, att) in atts) {
            val digest = att.jsonObject["digest"]?.jsonPrimitive?.content ?: continue
            if (io { blobs.has(digest) }) continue
            try {
                remote.readAttachment(id, name, rev) { stream -> io { blobs.put(digest, stream) } }
            } catch (e: AttachmentDigestException) {
                return false
            }
        }
        return true
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
            val docs =
                io {
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
            for (chunk in bySize(docs.filter { it.follows.isEmpty() }.map { it.json })) {
                val refused = remote.bulkDocsNoNewEdits(chunk)
                failures += refused
                total += chunk.size - refused.size
            }
            for (doc in docs.filter { it.follows.isNotEmpty() }) {
                val refused = remote.putWithAttachments(doc.id, doc.json, doc.follows) { digest -> blobs.open(digest) }
                if (refused == null) {
                    total++
                } else {
                    failures += refused
                }
            }
            since = changes.last().seq
            writeCheckpoint(pushId, cp.copy(seq = since.toString()))
            _state.value = SyncState.Pushing(total, io { store.countChangesSince(since) })
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
     * Keeps syncing until cancelled, waking on a local write or a remote change.
     * Remote changes come from a longpoll, or with [continuous] from one
     * streaming connection that stays open. Network errors, 429 and 5xx back
     * off exponentially up to [maxBackoffMs]; other errors (auth, 4xx) stop the loop.
     */
    suspend fun live(
        localWrites: Flow<Unit>,
        continuous: Boolean = false,
        longpollMs: Long = 60_000,
        debounceMs: Long = 2_000,
        maxBackoffMs: Long = 5 * 60_000,
    ) {
        coroutineScope {
            val remoteSignal = Channel<Unit>(Channel.CONFLATED)
            if (continuous) launch { followRemote(remoteSignal, maxBackoffMs) }
            var backoff = 1_000L
            while (isActive) {
                try {
                    sync()
                    backoff = 1_000L
                    val since = readCheckpoint(pullId).seq ?: "0"
                    coroutineScope {
                        val remoteChange =
                            async {
                                if (continuous) remoteSignal.receive() else remote.changes(since, 1, longpollMs, filter)
                            }
                        val localWrite =
                            async {
                                localWrites.first()
                                delay(debounceMs)
                            }
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
    }

    /** Keeps a continuous changes connection open, signalling [signal] for each change and each (re)connect. */
    private suspend fun followRemote(
        signal: SendChannel<Unit>,
        maxBackoffMs: Long,
    ) {
        var backoff = 1_000L
        while (true) {
            // Signal on every connect, so changes made while the connection was down aren't missed.
            signal.trySend(Unit)
            try {
                remote.continuousChanges("now", filter).collect { signal.trySend(Unit) }
                backoff = 1_000L
            } catch (e: IOException) {
                if (!ChangesFeed.isTransient(e)) throw e
            }
            delay(backoff)
            backoff = (backoff * 2).coerceAtMost(maxBackoffMs)
        }
    }

    private fun passesFilter(id: String): Boolean =
        when (val f = filter) {
            null -> true
            is SyncFilter.DocIds -> id in f.ids
            is SyncFilter.Selector -> store.get(id)?.let { Mango.matches(it.body, f.selector) } ?: true
        }

    /** A revision ready to send: its JSON, and the attachments that go as multipart parts. */
    private class WireDoc(val id: String, val json: JsonObject, val follows: List<PendingAttachment>)

    /**
     * Builds the wire form of a local revision: body plus `_id`, `_rev`,
     * `_deleted`, `_revisions`. Attachments the server has in one of
     * [serverAncestors] go as stubs. Small ones are inline base64; large ones
     * are marked `follows` and sent as their own part.
     */
    private fun toWireDoc(
        id: String,
        rev: String,
        serverAncestors: List<String>,
    ): WireDoc? {
        val doc = store.getRev(id, rev) ?: return null
        val path = store.revisionPath(id, rev).map(Rev::parse)
        val known = serverAncestors.map(Rev::parse).filter { it in path }.maxOfOrNull { it.gen } ?: 0
        val follows = mutableListOf<PendingAttachment>()
        val atts =
            (doc.body["_attachments"] as? JsonObject)?.mapValues { (name, a) ->
                val att = a.jsonObject
                val revpos = att["revpos"]?.jsonPrimitive?.int ?: Int.MAX_VALUE
                val digest = att["digest"]?.jsonPrimitive?.content
                if (digest == null || revpos <= known) return@mapValues att // the server has it: send the stub
                val missing = MissingAttachmentException("$id $rev: attachment content for $digest isn't stored locally")
                val length = att["length"]?.jsonPrimitive?.long ?: 0L
                if (length > inlineAttachmentBytes) {
                    if (!blobs.has(digest)) throw missing
                    val type = att["content_type"]?.jsonPrimitive?.content ?: "application/octet-stream"
                    follows += PendingAttachment(name, type, length, digest)
                    JsonObject(att.filterKeys { it !in setOf("stub", "digest") } + ("follows" to JsonPrimitive(true)))
                } else {
                    val bytes = blobs.get(digest) ?: throw missing
                    JsonObject(
                        att.filterKeys { it !in setOf("stub", "length", "digest") } +
                            ("data" to JsonPrimitive(Base64.getEncoder().encodeToString(bytes))),
                    )
                }
            }
        val body = if (atts == null) doc.body else JsonObject(doc.body + ("_attachments" to JsonObject(atts)))
        val json =
            JsonObject(
                body +
                    buildJsonObject {
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
        return WireDoc(id, json, follows)
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

    private suspend fun writeCheckpoint(
        id: String,
        cp: Checkpoint,
    ) {
        val body =
            buildJsonObject {
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
internal suspend fun putLocalWithRev(
    remote: RemoteDb,
    id: String,
    body: JsonObject,
    knownRev: String?,
): String {
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
