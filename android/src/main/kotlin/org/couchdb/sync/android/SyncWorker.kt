package org.couchdb.sync.android

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import org.couchdb.sync.AuthRequiredException
import org.couchdb.sync.ChangesFeed
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Runs one pull + push for the database named in its input. Output: `pulled`,
 * `pushed`, `push_failures` (count), `refused_ids` (first few ids), `pull_errors`.
 * Failure output: `error` (a code such as `auth_required`, `key_lost`, `http`
 * or `unexpected`) and `message`.
 */
class SyncWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val name = inputData.getString(KEY_DB) ?: return failure("no_database", null)
        return try {
            val db = CouchLiteAndroid.open(applicationContext, name, inputData.getBoolean(KEY_ENCRYPTED, false))
            val replicator =
                db.replicator(
                    CouchLiteAndroid.remote(name),
                    CouchLiteAndroid.filters?.invoke(name),
                    resolver = CouchLiteAndroid.resolvers?.invoke(name),
                )
            val r = replicator.sync()
            Result.success(
                workDataOf(
                    KEY_PULLED to r.pulled,
                    KEY_PUSHED to r.pushed,
                    KEY_PUSH_FAILURES to r.pushFailures.size,
                    KEY_REFUSED_IDS to r.pushFailures.take(MAX_REFUSED_IDS).joinToString(",") { it.id },
                    KEY_PULL_ERRORS to r.pullErrors,
                ),
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: AuthRequiredException) {
            failure("auth_required", e.message)
        } catch (e: KeyLostException) {
            failure("key_lost", e.message)
        } catch (e: IOException) {
            // Network errors, 429 and 5xx clear up; other HTTP errors (403, 404) won't.
            if (ChangesFeed.isTransient(e)) Result.retry() else failure("http", e.message)
        } catch (e: Exception) {
            failure("unexpected", e.message ?: e::class.java.simpleName)
        }
    }

    private fun failure(
        code: String,
        message: String?,
    ): Result = Result.failure(workDataOf(KEY_ERROR to code, KEY_MESSAGE to message.orEmpty()))

    internal companion object {
        const val KEY_DB = "db"
        const val KEY_ENCRYPTED = "encrypted"
        const val KEY_PULLED = "pulled"
        const val KEY_PUSHED = "pushed"
        const val KEY_PUSH_FAILURES = "push_failures"
        const val KEY_REFUSED_IDS = "refused_ids"
        const val KEY_PULL_ERRORS = "pull_errors"
        const val KEY_ERROR = "error"
        const val KEY_MESSAGE = "message"
        const val MAX_REFUSED_IDS = 10
    }
}

/** Schedules background sync with WorkManager. Jobs only run with a network connection. */
object SyncScheduler {
    /** Sync every [minutes] (WorkManager's minimum is 15). */
    fun schedulePeriodic(
        context: Context,
        db: String,
        minutes: Long = 15,
        unmeteredOnly: Boolean = false,
        encrypted: Boolean = false,
    ) {
        val req =
            PeriodicWorkRequestBuilder<SyncWorker>(minutes.coerceAtLeast(15), TimeUnit.MINUTES)
                .setConstraints(constraints(unmeteredOnly))
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .setInputData(workDataOf(SyncWorker.KEY_DB to db, SyncWorker.KEY_ENCRYPTED to encrypted))
                .addTag(tag(db))
                .build()
        WorkManager.getInstance(context)
            .enqueueUniquePeriodicWork("couchlite-periodic-$db", ExistingPeriodicWorkPolicy.UPDATE, req)
    }

    /** Sync once as soon as there's a network. Repeated calls while queued collapse into one. */
    fun syncNow(
        context: Context,
        db: String,
        encrypted: Boolean = false,
    ) {
        val req =
            OneTimeWorkRequestBuilder<SyncWorker>()
                .setConstraints(constraints(false))
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .setInputData(workDataOf(SyncWorker.KEY_DB to db, SyncWorker.KEY_ENCRYPTED to encrypted))
                .addTag(tag(db))
                .build()
        WorkManager.getInstance(context)
            .enqueueUniqueWork("couchlite-now-$db", ExistingWorkPolicy.KEEP, req)
    }

    /** Emits the sync jobs for [db] (periodic and one-off) whenever their state changes. */
    fun status(
        context: Context,
        db: String,
    ): Flow<List<WorkInfo>> = WorkManager.getInstance(context).getWorkInfosByTagFlow(tag(db))

    fun cancel(
        context: Context,
        db: String,
    ) {
        WorkManager.getInstance(context).cancelUniqueWork("couchlite-periodic-$db")
        WorkManager.getInstance(context).cancelUniqueWork("couchlite-now-$db")
    }

    private fun tag(db: String) = "couchlite:$db"

    private fun constraints(unmeteredOnly: Boolean) =
        Constraints.Builder()
            .setRequiredNetworkType(if (unmeteredOnly) NetworkType.UNMETERED else NetworkType.CONNECTED)
            .build()
}
