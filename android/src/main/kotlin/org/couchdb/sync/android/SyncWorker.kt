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
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import org.couchdb.sync.AuthRequiredException
import java.io.IOException
import java.util.concurrent.TimeUnit

/** Runs one pull + push for the database named in its input. */
class SyncWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val name = inputData.getString(KEY_DB) ?: return Result.failure()
        val db = CouchLiteAndroid.open(applicationContext, name, inputData.getBoolean(KEY_ENCRYPTED, false))
        return try {
            val r = db.replicator(CouchLiteAndroid.remote(name), CouchLiteAndroid.filters?.invoke(name)).sync()
            Result.success(workDataOf("pulled" to r.pulled, "pushed" to r.pushed))
        } catch (e: AuthRequiredException) {
            Result.failure(workDataOf("error" to "auth_required"))
        } catch (e: IOException) {
            Result.retry()
        }
    }

    internal companion object {
        const val KEY_DB = "db"
        const val KEY_ENCRYPTED = "encrypted"
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
        val req = PeriodicWorkRequestBuilder<SyncWorker>(minutes.coerceAtLeast(15), TimeUnit.MINUTES)
            .setConstraints(constraints(unmeteredOnly))
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .setInputData(workDataOf(SyncWorker.KEY_DB to db, SyncWorker.KEY_ENCRYPTED to encrypted))
            .build()
        WorkManager.getInstance(context)
            .enqueueUniquePeriodicWork("couchlite-periodic-$db", ExistingPeriodicWorkPolicy.UPDATE, req)
    }

    /** Sync once as soon as there's a network. Repeated calls while queued collapse into one. */
    fun syncNow(context: Context, db: String, encrypted: Boolean = false) {
        val req = OneTimeWorkRequestBuilder<SyncWorker>()
            .setConstraints(constraints(false))
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .setInputData(workDataOf(SyncWorker.KEY_DB to db, SyncWorker.KEY_ENCRYPTED to encrypted))
            .build()
        WorkManager.getInstance(context)
            .enqueueUniqueWork("couchlite-now-$db", ExistingWorkPolicy.KEEP, req)
    }

    fun cancel(context: Context, db: String) {
        WorkManager.getInstance(context).cancelUniqueWork("couchlite-periodic-$db")
        WorkManager.getInstance(context).cancelUniqueWork("couchlite-now-$db")
    }

    private fun constraints(unmeteredOnly: Boolean) = Constraints.Builder()
        .setRequiredNetworkType(if (unmeteredOnly) NetworkType.UNMETERED else NetworkType.CONNECTED)
        .build()
}
