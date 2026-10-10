package org.couchdb.sync.android

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import org.couchdb.sync.CouchLite
import org.couchdb.sync.FileBlobStore
import org.couchdb.sync.RemoteDb
import org.couchdb.sync.SqlLocalStore
import org.couchdb.sync.SyncFilter
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * Entry point on Android. Open databases once and reuse them:
 *
 * ```
 * // Application.onCreate
 * CouchLiteAndroid.remotes = { name -> RemoteDb("https://db.example.com/$name", Auth.Session(user, pw)) }
 * val notes = CouchLiteAndroid.open(this, "notes", encrypted = true)
 * SyncScheduler.schedulePeriodic(this, "notes")
 * ```
 */
object CouchLiteAndroid {
    private val open = ConcurrentHashMap<String, Pair<CouchLite, Boolean>>()

    /**
     * Builds the remote for a database name. Background sync ([SyncWorker])
     * calls this, so set it in `Application.onCreate` before any sync runs.
     * Keep credentials out of WorkManager input data; read them here instead.
     */
    @Volatile var remotes: ((String) -> RemoteDb)? = null

    /** Optional per-database sync filter used by background sync. */
    @Volatile var filters: ((String) -> SyncFilter?)? = null

    /**
     * Opens (or creates) a local database. With [encrypted], the database
     * file uses SQLCipher and attachments use AES-GCM, both keyed by a secret
     * protected by the Android Keystore. A database's [encrypted] setting
     * can't change after it's created; [destroy] it and sync again instead.
     *
     * Attachments live under `noBackupFilesDir`, so they aren't copied to
     * cloud backups. Opening an already-open database with a different
     * [encrypted] value throws.
     */
    fun open(context: Context, name: String, encrypted: Boolean = false): CouchLite {
        val (db, wasEncrypted) = open.getOrPut(name) { create(context, name, encrypted) to encrypted }
        check(wasEncrypted == encrypted) { "$name is already open with encrypted = $wasEncrypted" }
        return db
    }

    private fun create(context: Context, name: String, encrypted: Boolean): CouchLite {
        val app = context.applicationContext
        val dbFile = app.getDatabasePath("$name.couchlite.db").also { it.parentFile?.mkdirs() }
        val blobDir = File(app.noBackupFilesDir, "couchlite/$name")
        if (encrypted) {
            System.loadLibrary("sqlcipher")
            val secret = KeystorePassphrase.get(app, name)
            val db = net.zetetic.database.sqlcipher.SQLiteDatabase.openOrCreateDatabase(dbFile, secret, null, null)
            db.enableWriteAheadLogging()
            return CouchLite(SqlLocalStore(AndroidSqlDriver(db)), FileBlobStore(blobDir, secret))
        }
        val db = SQLiteDatabase.openOrCreateDatabase(dbFile, null)
        db.enableWriteAheadLogging()
        return CouchLite(SqlLocalStore(AndroidSqlDriver(db)), FileBlobStore(blobDir))
    }

    /** Deletes a local database, its attachments and its key. The server copy is untouched. */
    fun destroy(context: Context, name: String) {
        open.remove(name)
        val app = context.applicationContext
        SQLiteDatabase.deleteDatabase(app.getDatabasePath("$name.couchlite.db"))
        File(app.noBackupFilesDir, "couchlite/$name").deleteRecursively()
        KeystorePassphrase.forget(app, name)
    }

    internal fun remote(name: String): RemoteDb =
        checkNotNull(remotes) { "Set CouchLiteAndroid.remotes before syncing" }(name)
}
