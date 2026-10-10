package org.couchdb.sync.example.notes

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.couchdb.sync.CouchLite
import org.couchdb.sync.Query
import org.couchdb.sync.Replicator
import org.couchdb.sync.Resolution
import org.couchdb.sync.SyncState
import org.couchdb.sync.android.CouchLiteAndroid
import org.couchdb.sync.android.SyncScheduler
import java.util.UUID

/** What the sync screen shows. */
data class SyncStatus(
    val state: SyncState = SyncState.Idle,
    /** When a sync last finished. Set when the replicator goes back to idle after moving data. */
    val lastSynced: Long? = null,
    /** Changes the server refused in the last manual sync (for example, failed validate_doc_update). */
    val refused: List<String> = emptyList(),
    /** Why live sync stopped, if it did. */
    val error: String? = null,
)

/**
 * Holds the local database and the sync loop. Every edit goes to the local
 * database first, so the screens work with no connection; sync runs on top.
 */
class NotesViewModel(app: Application) : AndroidViewModel(app) {
    private val settings = (app as NotesApp).settings

    // Opened once and shared with SyncWorker. Encryption must match there.
    private val db: CouchLite = CouchLiteAndroid.open(app, DB_NAME, encrypted = true)

    private var activeReplicator: Replicator? = null
    private var liveJob: Job? = null

    private val _configured = MutableStateFlow(settings.config != null)
    val configured: StateFlow<Boolean> = _configured.asStateFlow()

    private val _status = MutableStateFlow(SyncStatus())
    val status: StateFlow<SyncStatus> = _status.asStateFlow()

    /** Notes with pinned ones first, then newest first. Updates on local edits and on sync. */
    val notes: StateFlow<List<Note>> =
        db.observe(
            Query(
                selector = buildJsonObject { put("type", "note") },
                sort = listOf("pinned" to false, "updatedAt" to false),
            ),
        ).map { docs -> docs.map { Note.from(it) } }
            .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    init {
        // An index on "type" keeps the notes query fast as the database grows.
        viewModelScope.launch { db.createIndex("type") }
    }

    fun currentConfig(): ServerConfig? = settings.config

    fun newNoteId(): String = UUID.randomUUID().toString()

    // ---- Notes ----

    /** The note as stored now, or null if there isn't one. */
    suspend fun load(id: String): Note? = db.get(id)?.let { Note.from(it) }

    /** Other versions left by conflicting edits. Re-checked each time the note changes. */
    fun conflicts(id: String): Flow<List<Note>> = db.observeConflicts(id).map { docs -> docs.map { Note.from(it) } }

    /**
     * Saves [title] and [body] on top of [baseRev], the revision the editor
     * started from (null for a new note). If that revision is no longer the
     * latest, this throws [org.couchdb.sync.ConflictException] instead of
     * overwriting the other device's change.
     */
    suspend fun save(
        id: String,
        baseRev: String?,
        title: String,
        body: String,
        pinned: Boolean,
    ) {
        db.put(id, noteBody(title, body, pinned), baseRev)
    }

    suspend fun delete(
        id: String,
        rev: String,
    ) {
        db.delete(id, rev)
    }

    /**
     * Settles a conflict. [chosenRev] null keeps the version the server
     * picked; otherwise the chosen losing version is written on top of it.
     * The losing branches are deleted, and the result syncs like any edit.
     */
    suspend fun resolve(
        id: String,
        chosenRev: String?,
    ) {
        db.resolve(id) { _, others ->
            val chosen = others.firstOrNull { it.rev == chosenRev }
            if (chosen == null) {
                Resolution.KeepWinner
            } else {
                // Stamp the time so the chosen version sorts as the newest edit.
                Resolution.Merge(JsonObject(chosen.body + ("updatedAt" to JsonPrimitive(System.currentTimeMillis()))))
            }
        }
    }

    // ---- Connection ----

    /**
     * Checks the server and credentials, creating the database if needed,
     * then saves the settings and starts sync. Returns an error message, or
     * null on success.
     */
    suspend fun connect(config: ServerConfig): String? =
        try {
            config.remote().ensureExists(create = true)
            settings.config = config
            activeReplicator = null
            SyncScheduler.schedulePeriodic(getApplication(), DB_NAME, encrypted = true)
            _configured.value = true
            startLiveSync()
            null
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            e.message ?: "Could not connect"
        }

    // ---- Sync ----

    /**
     * Syncs while the screen is visible. Local edits go out right away and
     * server changes come in as they happen. Does nothing until a server is set up.
     */
    fun startLiveSync() {
        val replicator = currentReplicator() ?: return
        if (liveJob?.isActive == true) return
        _status.update { it.copy(error = null) }
        liveJob =
            viewModelScope.launch {
                val watcher =
                    launch {
                        replicator.state.collect { s ->
                            _status.update { cur ->
                                // Idle after a sync that was running (or failing) means that sync finished.
                                val finished = s == SyncState.Idle && cur.state != SyncState.Idle
                                cur.copy(state = s, lastSynced = if (finished) System.currentTimeMillis() else cur.lastSynced)
                            }
                        }
                    }
                try {
                    replicator.live(db.localWrites.map { })
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Only errors that won't clear up on their own (bad login, a 404) end up here.
                    _status.update { it.copy(error = e.message ?: "Sync stopped") }
                } finally {
                    watcher.cancel()
                }
            }
    }

    fun stopLiveSync() {
        liveJob?.cancel()
        liveJob = null
    }

    /**
     * Asks WorkManager to push pending changes once there's a connection. It
     * runs after the app leaves the screen, so edits made offline still sync.
     */
    fun syncInBackground() {
        if (settings.config != null) SyncScheduler.syncNow(getApplication(), DB_NAME, encrypted = true)
    }

    /** One sync now, in the foreground. Shows what the server refused. */
    fun syncNow() {
        val replicator = currentReplicator() ?: return
        viewModelScope.launch {
            _status.update { it.copy(error = null) }
            try {
                val result = replicator.sync()
                _status.update { cur ->
                    cur.copy(refused = result.pushFailures.map { "${it.id}: ${it.reason ?: it.error}" })
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                _status.update { it.copy(error = e.message ?: "Sync failed") }
            }
        }
    }

    private fun currentReplicator(): Replicator? {
        activeReplicator?.let { return it }
        val config = settings.config ?: return null
        return db.replicator(config.remote()).also { activeReplicator = it }
    }
}
