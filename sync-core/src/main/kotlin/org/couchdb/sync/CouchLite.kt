package org.couchdb.sync

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** Decides what a conflicted document should become. */
fun interface ConflictResolver {
    /** [winner] is the revision CouchDB picked; [others] are the losing leaves. */
    suspend fun resolve(
        winner: Doc,
        others: List<Doc>,
    ): Resolution
}

sealed interface Resolution {
    /** Keep the winner's body and delete the other branches. */
    data object KeepWinner : Resolution

    /** Write [body] as the new winner and delete the other branches. */
    data class Merge(
        val body: JsonObject,
    ) : Resolution

    /** Make the losing revision [rev] the new winner, with its body. */
    data class Pick(
        val rev: String,
    ) : Resolution
}

data class AttachmentInfo(
    val name: String,
    val contentType: String,
    val length: Long,
    val digest: String,
)

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
    suspend fun put(
        id: String,
        body: JsonObject,
        rev: String? = null,
    ): String = withContext(io) { store.put(id, body, rev) }.also { writes.tryEmit(id) }

    suspend fun delete(
        id: String,
        rev: String,
    ): String = withContext(io) { store.delete(id, rev) }.also { writes.tryEmit(id) }

    // ---- Queries ----

    /** Adds an index on a field (dotted paths allowed). Queries work without one; this makes them faster. */
    suspend fun createIndex(field: String) = withContext(io) { store.createIndex(field) }

    /**
     * Adds an index over several fields. Sorting by a prefix of them, all in
     * one direction, reads the index instead of sorting in memory.
     */
    suspend fun createCompoundIndex(fields: List<String>) = withContext(io) { store.createCompoundIndex(fields) }

    suspend fun find(query: Query): List<Doc> = withContext(io) { store.find(query) }

    /** Emits the query's results now and again whenever any doc changes. */
    fun observe(query: Query): Flow<List<Doc>> = refresh { store.find(query) }

    /** Emits a doc now and after each change to it (null when missing or deleted). */
    fun observe(id: String): Flow<Doc?> = refresh(id) { store.get(id) }

    /**
     * Runs [read] now, then again after each change to [id] (or to any doc,
     * when [id] is null). Subscribes before the first read, so a write that
     * lands during it still triggers another read.
     */
    private fun <T> refresh(
        id: String? = null,
        read: () -> T,
    ): Flow<T> =
        changed
            .onSubscription { emit(id.orEmpty()) }
            .filter { id == null || it == id }
            .conflate()
            .map { read() }
            .distinctUntilChanged()
            .flowOn(io)

    // ---- Attachments ----

    /** Adds or replaces an attachment on revision [rev]. Returns the new rev. */
    suspend fun putAttachment(
        id: String,
        rev: String,
        name: String,
        contentType: String,
        data: ByteArray,
    ): String =
        withContext(io) {
            val doc = store.getRev(id, rev) ?: throw ConflictException("$id: unknown revision $rev")
            val digest = attachmentDigest(data)
            blobs.put(digest, data)
            val stub =
                JsonObject(
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

    suspend fun deleteAttachment(
        id: String,
        rev: String,
        name: String,
    ): String =
        withContext(io) {
            val doc = store.getRev(id, rev) ?: throw ConflictException("$id: unknown revision $rev")
            val atts = (doc.body["_attachments"] as? JsonObject).orEmpty() - name
            val body =
                if (atts.isEmpty()) {
                    JsonObject(
                        doc.body - "_attachments",
                    )
                } else {
                    JsonObject(doc.body + ("_attachments" to JsonObject(atts)))
                }
            store.put(id, body, rev)
        }.also { writes.tryEmit(id) }

    /** Attachment content from the winning revision, or null if missing or not downloaded. */
    suspend fun getAttachment(
        id: String,
        name: String,
    ): ByteArray? =
        withContext(io) {
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

    /** Ids of docs that have conflicts. */
    suspend fun conflictIds(): List<String> = withContext(io) { store.conflictIds() }

    /** The losing revisions of a doc, updated whenever it changes. */
    fun observeConflicts(id: String): Flow<List<Doc>> = refresh(id) { store.conflicts(id) }

    /** Ids of docs with conflicts, updated whenever any doc changes. */
    fun observeConflictIds(): Flow<List<String>> = refresh { store.conflictIds() }

    /**
     * Resolves a conflict: deletes every losing branch and writes the chosen
     * body on top of the winner, in one transaction. The result replicates
     * like any other edit. Returns the new winning rev.
     */
    suspend fun resolve(
        id: String,
        resolver: ConflictResolver,
    ): String? = resolveConflict(store, io, id, resolver)

    // ---- Maintenance ----

    /** Drops old revision bodies and history (see [SqlLocalStore.compact]) and unused attachment files. */
    suspend fun compact(revsLimit: Int = 1000) =
        withContext(io) {
            val used = store.compact(revsLimit)
            (blobs.digests() - used).forEach(blobs::delete)
        }

    /** Closes the local database. Later calls fail. */
    fun close() = store.close()

    // ---- Sync ----

    /** [resolver] settles conflicts automatically after each pull. */
    fun replicator(
        remote: RemoteDb,
        filter: SyncFilter? = null,
        batchSize: Int = 100,
        resolver: ConflictResolver? = null,
    ) = Replicator(store, remote, blobs, filter, batchSize, io, resolver = resolver)

    /**
     * Syncs until cancelled, waking on local writes and remote changes. With
     * [continuous], one streaming connection watches for remote changes;
     * otherwise each wait is a longpoll.
     */
    suspend fun liveSync(
        remote: RemoteDb,
        filter: SyncFilter? = null,
        continuous: Boolean = false,
        resolver: ConflictResolver? = null,
    ) = replicator(remote, filter, resolver = resolver).live(localWrites.map { }, continuous = continuous)
}

/**
 * Settles a doc's conflicts with [resolver]: deletes the losing branches and
 * writes the chosen body on top of the winner, in one transaction. Returns
 * the new winning rev, or the current one if there's nothing to resolve.
 */
internal suspend fun resolveConflict(
    store: SqlLocalStore,
    io: CoroutineDispatcher,
    id: String,
    resolver: ConflictResolver,
): String? {
    val (winRev, others) = withContext(io) { store.winningRev(id) to store.conflicts(id) }
    if (winRev == null || others.isEmpty()) return winRev
    val winner = withContext(io) { store.getRev(id, winRev) } ?: return winRev
    val resolution = resolver.resolve(winner, others)
    return withContext(io) {
        store.transaction {
            if (store.winningRev(id) != winRev) throw ConflictException("$id changed while resolving")
            val body =
                when (resolution) {
                    Resolution.KeepWinner -> null
                    is Resolution.Merge -> resolution.body
                    is Resolution.Pick ->
                        others.firstOrNull { it.rev == resolution.rev }?.body
                            ?: throw IllegalArgumentException("${resolution.rev} is not a conflicting revision of $id")
                }
            others.forEach { store.delete(id, it.rev) }
            if (body == null) winRev else store.put(id, body, winRev)
        }
    }
}
