package org.couchdb.sync.example.notes

import android.app.Application
import org.couchdb.sync.android.CouchLiteAndroid
import org.couchdb.sync.android.SyncScheduler

class NotesApp : Application() {
    lateinit var settings: Settings
        private set

    override fun onCreate() {
        super.onCreate()
        settings = Settings(this)

        // Background sync (WorkManager) has no screen to ask, so it gets its
        // server from here. It reads the saved settings each time it runs.
        CouchLiteAndroid.remotes = { name -> settings.config?.remote(name) ?: error("No server configured") }

        // Sync every 15 minutes whenever there's a connection, once the app is set up.
        if (settings.config != null) SyncScheduler.schedulePeriodic(this, DB_NAME, encrypted = true)
    }
}
