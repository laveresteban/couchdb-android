package org.couchdb.sync.android

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import org.couchdb.sync.CouchLite
import org.couchdb.sync.RemoteDb
import org.couchdb.sync.SqlLocalStore
import java.util.concurrent.ConcurrentHashMap

/**
 * Entry point on Android. Open databases once and reuse them:
 *
 * ```
 * // Application.onCreate
 * CouchLiteAndroid.remotes = { name -> RemoteDb("https://db.example.com/$name", Auth.Session(user, pw)) }
 * val notes = CouchLiteAndroid.open(this, "notes")
 * SyncScheduler.schedulePeriodic(this, "notes")
 * ```
 */
object CouchLiteAndroid {
    private val open = ConcurrentHashMap<String, CouchLite>()

    /**
     * Builds the remote for a database name. Background sync ([SyncWorker])
     * calls this, so set it in `Application.onCreate` before any sync runs.
     * Keep credentials out of WorkManager input data; read them here instead.
     */
    @Volatile var remotes: ((String) -> RemoteDb)? = null

    fun open(context: Context, name: String): CouchLite = open.getOrPut(name) {
        val helper = object : SQLiteOpenHelper(context.applicationContext, "$name.couchlite.db", null, 1) {
            override fun onConfigure(db: SQLiteDatabase) {
                db.enableWriteAheadLogging()
            }
            override fun onCreate(db: SQLiteDatabase) = Unit // SqlLocalStore creates its tables
            override fun onUpgrade(db: SQLiteDatabase, old: Int, new: Int) = Unit
        }
        CouchLite(SqlLocalStore(AndroidSqlDriver(helper.writableDatabase)))
    }

    internal fun remote(name: String): RemoteDb =
        checkNotNull(remotes) { "Set CouchLiteAndroid.remotes before syncing" }(name)
}
