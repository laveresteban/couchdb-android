package org.couchdb.sync

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** Decides what a conflicted document should become. */
fun interface ConflictResolver {
    /** [winner] is the revision CouchDB picked; [others] are the losing leaves. */
    suspend fun resolve(winner: Doc, others: List<Doc>): Resolution
}

sealed interface Resolution {
    /** Keep the winner's body and delete the other branches. */
    data object KeepWinner : Resolution

    /** Write [body] as the new winner and delete the other branches. */
    data class Merge(val body: JsonObject) : Resolution
}

data class AttachmentInfo(val name: String, val contentType: String, val length: Long, val digest: String)

/**
 * An offline-first database. Every read and write is local; [replicator]
 * syncs with a CouchDB database when the network allows.
 */
class CouchLite(
    val store: SqlLocalStore,
    val blobs: BlobStore = MemoryBlobStore(),
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val writes = MutableSharedFlow<String>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    private val changed = MutableSharedFlow<String>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    init {
        store.onChange = { changed.tryEmit(it) }
    }

    /** Ids of documents written locally, as they happen. */
    val localWrites: Flow<String> = writes.asSharedFlow()

    /** Ids of documents changed by local writes or by replication. */
    val changes: Flow<String> = changed.asSharedFlow()

    suspend fun get(id: String): Doc? = withContext(io) { store.get(id) }

    suspend fun allDocs(): List<Doc> = withContext(io) { store.allDocs() }

    /**
     * Creates or updates a doc. Pass the current [rev] to update. Throws
     * [ConflictException] on a stale rev. As in CouchDB, attachments are kept
     * only if [body] includes their `_attachments` stubs (copy them from the
     * doc you read).
     */
    suspend fun put(id: String, body: JsonObject, rev: String? = null): String =
        withContext(io) { store.put(id, body, rev) }.also { writes.tryEmit(id) }

    suspend fun delete(id: String, rev: String): String =
        withContext(io) { store.delete(id, rev) }.also { writes.tryEmit(id) }

    // ---- Queries ----

    /** Adds an index on a field (dotted paths allowed). Queries work without one; this makes them faster. */
    suspend fun createIndex(field: String) = withContext(io) { store.createIndex(field) }

    suspend fun find(query: Query): List<Doc> = withContext(io) { store.find(query) }

    /** Emits the query's results now and again whenever any doc changes. */
    fun observe(query: Query): Flow<List<Doc>> = flow {
        emit(Unit)
        changed.collect { emit(Unit) }
    }.conflate().map { store.find(query) }.distinctUntilChanged().flowOn(io)

    /** Emits a doc now and after each change to it (null when missing or deleted). */
    fun observe(id: String): Flow<Doc?> = flow {
        emit(Unit)
        changed.collect { if (it == id) emit(Unit) }
    }.conflate().map { store.get(id) }.distinctUntilChanged().flowOn(io)

    // ---- Attachments ----

    /** Adds or replaces an attachment on revision [rev]. Returns the new rev. */
    suspend fun putAttachment(id: String, rev: String, name: String, contentType: String, data: ByteArray): String =
        withContext(io) {
            val doc = store.getRev(id, rev) ?: throw ConflictException("$id: unknown revision $rev")
            val digest = attachmentDigest(data)
            blobs.put(digest, data)
            val stub = JsonObject(
                mapOf(
                    "content_type" to JsonPrimitive(contentType),
                    "digest" to JsonPrimitive(digest),
                    "length" to JsonPrimitive(data.size),
                    "revpos" to JsonPrimitive(Rev.parse(rev).gen + 1),
                    "stub" to JsonPrimitive(true),
                ),
            )
            val atts = (doc.body["_attachments"] as? JsonObject).orEmpty() + (name to stub)
            store.put(id, JsonObject(doc.body + ("_attachments" to JsonObject(atts))), rev)
        }.also { writes.tryEmit(id) }

    suspend fun deleteAttachment(id: String, rev: String, name: String): String =
        withContext(io) {
            val doc = store.getRev(id, rev) ?: throw ConflictException("$id: unknown revision $rev")
            val atts = (doc.body["_attachments"] as? JsonObject).orEmpty() - name
            val body = if (atts.isEmpty()) JsonObject(doc.body - "_attachments") else JsonObject(doc.body + ("_attachments" to JsonObject(atts)))
            store.put(id, body, rev)
        }.also { writes.tryEmit(id) }

    /** Attachment content from the winning revision, or null if missing or not downloaded. */
    suspend fun getAttachment(id: String, name: String): ByteArray? = withContext(io) {
        attachments(store.get(id) ?: return@withContext null).firstOrNull { it.name == name }?.let { blobs.get(it.digest) }
    }

    fun attachments(doc: Doc): List<AttachmentInfo> =
        (doc.body["_attachments"] as? JsonObject).orEmpty().map { (name, a) ->
            val o = a.jsonObject
            AttachmentInfo(
                name,
                o["content_type"]?.jsonPrimitive?.content ?: "application/octet-stream",
                o["length"]?.jsonPrimitive?.content?.toLongOrNull() ?: 0,
                o["digest"]!!.jsonPrimitive.content,
            )
        }

    // ---- Conflicts ----

    /** Losing, non-deleted revisions of a doc. Empty means no conflict. */
    suspend fun conflicts(id: String): List<Doc> = withContext(io) { store.conflicts(id) }

    /**
     * Resolves a conflict: deletes every losing branch and, for
     * [Resolution.Merge], writes the merged body on top of the winner, in
     * one transaction. The result replicates like any other edit. Returns the
     * new winning rev.
     */
    suspend fun resolve(id: String, resolver: ConflictResolver): String? {
        val (winner, others) = withContext(io) { store.get(id) to store.conflicts(id) }
        if (winner == null || others.isEmpty()) return winner?.rev
        val resolution = resolver.resolve(winner, others)
        return withContext(io) {
            store.transaction {
                if (store.get(id)?.rev != winner.rev) throw ConflictException("$id changed while resolving")
                others.forEach { store.delete(id, it.rev) }
                when (resolution) {
                    Resolution.KeepWinner -> winner.rev
                    is Resolution.Merge -> store.put(id, resolution.body, winner.rev)
                }
            }
        }.also { writes.tryEmit(id) }
    }

    // ---- Maintenance ----

    /** Drops old revision bodies and history (see [SqlLocalStore.compact]) and unused attachment files. */
    suspend fun compact(revsLimit: Int = 1000) = withContext(io) {
        val used = store.compact(revsLimit)
        (blobs.digests() - used).forEach(blobs::delete)
    }

    // ---- Sync ----

    fun replicator(remote: RemoteDb, filter: SyncFilter? = null, batchSize: Int = 100) =
        Replicator(store, remote, blobs, filter, batchSize, io)

    /** Syncs until cancelled, waking on remote changes and local writes. */
    suspend fun liveSync(remote: RemoteDb, filter: SyncFilter? = null) =
        replicator(remote, filter).live(localWrites.map { })
}
