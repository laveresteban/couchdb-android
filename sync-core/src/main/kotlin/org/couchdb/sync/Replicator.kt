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
import java.util.UUID

sealed interface SyncState {
    data object Idle : SyncState
    data class Pulling(val docs: Int) : SyncState
    data class Pushing(val docs: Int) : SyncState
    data class Error(val error: Throwable) : SyncState
}

data class SyncResult(val pulled: Int, val pushed: Int)

/**
 * Two-way replication between a [SqlLocalStore] and a CouchDB database,
 * following the CouchDB replication protocol: read changes, diff revisions,
 * copy only the missing ones with their history, then checkpoint.
 *
 * Checkpoints are saved on both sides (local `local_docs` and remote
 * `_local/<id>`) with a shared session id. If the two disagree, for example
 * after the server database was recreated, replication starts from 0; the
 * revision diff makes that safe, just slower.
 */
class Replicator(
    private val store: SqlLocalStore,
    private val remote: RemoteDb,
    private val batchSize: Int = 100,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val _state = MutableStateFlow<SyncState>(SyncState.Idle)
    val state: StateFlow<SyncState> = _state.asStateFlow()

    private val pullId = md5Hex("${store.uuid}|${remote.url}|pull")
    private val pushId = md5Hex("${store.uuid}|${remote.url}|push")

    /** One pull then one push. Returns how many revisions moved each way. */
    suspend fun sync(createRemote: Boolean = false): SyncResult {
        remote.ensureExists(createRemote)
        return try {
            val pulled = pull()
            val pushed = push()
            _state.value = SyncState.Idle
            SyncResult(pulled, pushed)
        } catch (e: Exception) {
            _state.value = SyncState.Error(e)
            throw e
        }
    }

    /** Copies remote revisions this store is missing. */
    suspend fun pull(): Int {
        val cp = readCheckpoint(pullId)
        var since = cp.seq ?: "0"
        var total = 0
        while (true) {
            val page = remote.changes(since, batchSize)
            val wanted = io { store.missingRevs(page.results.associate { it.id to it.revs }) }
            val fetched = remote.bulkGet(wanted.flatMap { (id, revs) -> revs.map { id to it } })
            io {
                for (f in fetched) {
                    val doc = f.doc ?: continue
                    store.insertRevision(f.id, history(doc), doc, doc["_deleted"]?.jsonPrimitive?.boolean == true)
                }
            }
            total += fetched.count { it.doc != null }
            _state.value = SyncState.Pulling(total)
            if (page.results.isEmpty()) break
            since = page.lastSeq
            writeCheckpoint(pullId, cp.copy(seq = since))
            if (page.results.size < batchSize) break
        }
        return total
    }

    /** Sends local revisions the remote is missing. */
    suspend fun push(): Int {
        val cp = readCheckpoint(pushId)
        var since = cp.seq?.toLong() ?: 0L
        var total = 0
        while (true) {
            val changes = io { store.changesSince(since, batchSize) }
            if (changes.isEmpty()) break
            val leaves = io { changes.associate { it.id to store.leafRevs(it.id) } }
            val missing = remote.revsDiff(leaves)
            val docs = io { missing.flatMap { (id, revs) -> revs.mapNotNull { toWireDoc(id, it) } } }
            remote.bulkDocsNoNewEdits(docs)
            total += docs.size
            _state.value = SyncState.Pushing(total)
            since = changes.last().seq
            writeCheckpoint(pushId, cp.copy(seq = since.toString()))
        }
        return total
    }

    /**
     * Keeps syncing until cancelled: waits for a remote change (longpoll) or
     * a local write, then syncs again. Network errors back off exponentially
     * up to [maxBackoffMs]; auth errors stop the loop.
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
                    val remoteChange = async { remote.changes(since, 1, longpollMs) }
                    val localWrite = async { localWrites.first(); delay(debounceMs) }
                    select {
                        remoteChange.onAwait {}
                        localWrite.onAwait {}
                    }
                    remoteChange.cancel()
                    localWrite.cancel()
                }
            } catch (e: AuthRequiredException) {
                _state.value = SyncState.Error(e)
                throw e
            } catch (e: IOException) {
                _state.value = SyncState.Error(e)
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(maxBackoffMs)
            }
        }
    }

    /** Builds the wire form of a local revision: body plus `_id`, `_rev`, `_deleted`, `_revisions`. */
    private fun toWireDoc(id: String, rev: String): JsonObject? {
        val doc = store.getRev(id, rev) ?: return null
        val path = store.revisionPath(id, rev).map(Rev::parse)
        return JsonObject(
            doc.body + buildJsonObject {
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
        // _local docs on the server need the current _rev to be updated.
        val current = remote.getLocal(id)?.get("_rev")?.jsonPrimitive?.contentOrNull
        remote.putLocal(id, JsonObject(body + (current?.let { mapOf("_rev" to JsonPrimitive(it)) } ?: emptyMap())))
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
