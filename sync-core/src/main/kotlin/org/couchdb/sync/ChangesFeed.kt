package org.couchdb.sync

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.io.IOException
import kotlin.math.min
import kotlin.random.Random

/**
 * Follows a database's changes feed and hands each change to the collector,
 * for apps that react to server changes without keeping a local copy.
 *
 * - Longpoll by default; [continuous] keeps one streaming connection open.
 * - With a [checkpoint] name, the position is saved in `_local/<checkpoint>`
 *   on the server, so a restarted reader with the same name picks up where
 *   the last one stopped. Longpoll saves after each batch; continuous saves
 *   every [batchSize] changes, when the server ends the stream, and on [stop].
 *   Delivery is at-least-once: changes since the last save can repeat if the
 *   reader dies.
 * - Network errors, 429 and 5xx are retried with jittered exponential
 *   backoff (honoring `Retry-After`), up to [maxBackoffMs] between tries and
 *   [maxRetries] in a row if set. Other errors end the flow.
 *
 * Call [stop] from the collector to end the flow after the current change,
 * with its position saved. Cancelling the collector also ends it, but the
 * change being handled at that moment isn't checkpointed.
 */
class ChangesFeed(
    private val remote: RemoteDb,
    private val since: String = "0",
    private val checkpoint: String? = null,
    private val continuous: Boolean = false,
    private val filter: SyncFilter? = null,
    private val batchSize: Int = 500,
    private val longpollMs: Long = 60_000,
    private val heartbeatMs: Long = 30_000,
    private val maxRetries: Int? = null,
    private val maxBackoffMs: Long = 30_000,
    private val sleep: suspend (Long) -> Unit = { delay(it) },
) {
    @Volatile private var stopped = false
    private var checkpointRev: String? = null

    /** Ends the flow once the change being handled has been checkpointed. */
    fun stop() {
        stopped = true
    }

    fun asFlow(): Flow<RemoteChange> =
        flow {
            stopped = false
            var seq = checkpoint?.let { loadCheckpoint(it) } ?: since
            var saved = seq
            var failures = 0
            while (!stopped) {
                try {
                    if (continuous) {
                        var pending = 0
                        remote.continuousChanges(seq, filter, heartbeatMs).collect { change ->
                            failures = 0
                            emit(change)
                            change.seq?.let { seq = it } // null with seq_interval
                            if (stopped || ++pending >= batchSize) {
                                if (seq != saved) save(seq).also { saved = seq }
                                pending = 0
                            }
                            if (stopped) throw Stop
                        }
                        if (seq != saved) save(seq).also { saved = seq }
                        // Server closed the stream; reconnect from the last position.
                    } else {
                        val page = remote.changes(seq, batchSize, longpollMs, filter)
                        failures = 0
                        for (change in page.results) {
                            emit(change)
                            if (stopped) {
                                change.seq?.let {
                                    seq = it
                                    save(it)
                                }
                                return@flow
                            }
                        }
                        if (page.lastSeq != seq) {
                            seq = page.lastSeq
                            save(seq)
                            saved = seq
                        }
                    }
                } catch (e: Stop) {
                    return@flow
                } catch (e: IOException) {
                    if (!isTransient(e)) throw e
                    failures++
                    if (maxRetries != null && failures > maxRetries) throw e
                    sleep(backoffMs(e, failures))
                }
            }
        }

    private object Stop : RuntimeException() {
        private fun readResolve(): Any = Stop
    }

    private suspend fun loadCheckpoint(name: String): String? {
        val doc = remote.getLocal(name) ?: return null
        checkpointRev = doc["_rev"]?.jsonPrimitive?.contentOrNull
        return doc["since"]?.jsonPrimitive?.contentOrNull
    }

    private suspend fun save(seq: String) {
        val name = checkpoint ?: return
        checkpointRev = putLocalWithRev(remote, name, JsonObject(mapOf("since" to JsonPrimitive(seq))), checkpointRev)
    }

    private fun backoffMs(
        e: IOException,
        attempt: Int,
    ): Long {
        (e as? CouchException)?.retryAfterSeconds?.let { return it * 1000 }
        val exp = min(maxBackoffMs.toDouble(), 1000.0 * (1L shl min(attempt, 20)))
        return (exp * Random.nextDouble(0.5, 1.0)).toLong()
    }

    companion object {
        /** Network failures, 429 and 5xx are worth retrying; other HTTP errors aren't. */
        fun isTransient(e: Throwable): Boolean =
            when (e) {
                is AuthRequiredException -> false
                is CouchException -> e.status == 429 || e.status >= 500
                is IOException -> true
                else -> false
            }
    }
}
