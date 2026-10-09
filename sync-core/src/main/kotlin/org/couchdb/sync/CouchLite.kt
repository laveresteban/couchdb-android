package org.couchdb.sync

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject

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

/**
 * An offline-first database. Every read and write is local; [replicator]
 * syncs with a CouchDB database when the network allows.
 */
class CouchLite(
    val store: SqlLocalStore,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val writes = MutableSharedFlow<String>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    /** Ids of documents written locally, as they happen. */
    val localWrites: Flow<String> = writes.asSharedFlow()

    suspend fun get(id: String): Doc? = withContext(io) { store.get(id) }

    suspend fun allDocs(): List<Doc> = withContext(io) { store.allDocs() }

    /** Creates or updates a doc. Pass the current [rev] to update. Throws [ConflictException] on a stale rev. */
    suspend fun put(id: String, body: JsonObject, rev: String? = null): String =
        withContext(io) { store.put(id, body, rev) }.also { writes.tryEmit(id) }

    suspend fun delete(id: String, rev: String): String =
        withContext(io) { store.delete(id, rev) }.also { writes.tryEmit(id) }

    /** Losing, non-deleted revisions of a doc. Empty means no conflict. */
    suspend fun conflicts(id: String): List<Doc> = withContext(io) { store.conflicts(id) }

    /**
     * Resolves a conflict: deletes every losing branch and, for
     * [Resolution.Merge], writes the merged body on top of the winner.
     * The result replicates like any other edit. Returns the new winning rev.
     */
    suspend fun resolve(id: String, resolver: ConflictResolver): String? {
        val (winner, others) = withContext(io) { store.get(id) to store.conflicts(id) }
        if (winner == null || others.isEmpty()) return winner?.rev
        val resolution = resolver.resolve(winner, others)
        return withContext(io) {
            store.run {
                others.forEach { delete(id, it.rev) }
                when (resolution) {
                    Resolution.KeepWinner -> winner.rev
                    is Resolution.Merge -> put(id, resolution.body, winner.rev)
                }
            }
        }.also { writes.tryEmit(id) }
    }

    fun replicator(remote: RemoteDb, batchSize: Int = 100) = Replicator(store, remote, batchSize, io)

    /** Syncs until cancelled, waking on remote changes and local writes. */
    suspend fun liveSync(remote: RemoteDb) = replicator(remote).live(localWrites.map { })
}
