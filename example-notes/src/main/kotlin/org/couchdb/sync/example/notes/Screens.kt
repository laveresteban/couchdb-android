package org.couchdb.sync.example.notes

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import org.couchdb.sync.ConflictException
import org.couchdb.sync.SyncState
import java.text.DateFormat
import java.util.Date

/** Server URL, user and password. Checks them before saving anything. */
@Composable
fun SetupScreen(
    vm: NotesViewModel,
    onDone: () -> Unit,
) {
    val current = remember { vm.currentConfig() }
    var url by rememberSaveable { mutableStateOf(current?.url ?: "http://10.0.2.2:5984") }
    var user by rememberSaveable { mutableStateOf(current?.user ?: "") }
    var password by remember { mutableStateOf(current?.password ?: "") }
    var error by remember { mutableStateOf<String?>(null) }
    var busy by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    Column(
        modifier =
            Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(24.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("Connect to CouchDB", style = MaterialTheme.typography.headlineSmall)
        Text(
            "Notes are saved on this device first, so the app works offline. " +
                "Connect to a CouchDB server to keep them in sync.",
            style = MaterialTheme.typography.bodyMedium,
        )
        OutlinedTextField(
            value = url,
            onValueChange = { url = it },
            label = { Text("Server URL") },
            supportingText = { Text("Emulator: http://10.0.2.2:5984 reaches CouchDB on this computer.") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth(),
        )
        OutlinedTextField(
            value = user,
            onValueChange = { user = it },
            label = { Text("User name") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth(),
        )
        OutlinedTextField(
            value = password,
            onValueChange = { password = it },
            label = { Text("Password") },
            singleLine = true,
            visualTransformation = PasswordVisualTransformation(),
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
            modifier = Modifier.fillMaxWidth(),
        )
        Button(
            enabled = !busy && url.isNotBlank() && user.isNotBlank() && password.isNotBlank(),
            onClick = {
                busy = true
                error = null
                scope.launch {
                    error = vm.connect(ServerConfig(url.trim(), user.trim(), password))
                    busy = false
                    if (error == null) onDone()
                }
            },
        ) {
            Text(if (busy) "Connecting…" else "Connect")
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
    }
}

/** The list of notes, with the sync status on top. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun NotesScreen(
    vm: NotesViewModel,
    onOpen: (String) -> Unit,
    onNew: () -> Unit,
    onServer: () -> Unit,
) {
    val notes by vm.notes.collectAsStateWithLifecycle()
    val status by vm.status.collectAsStateWithLifecycle()

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Notes") },
                actions = {
                    TextButton(onClick = onServer) { Text("Server") }
                    TextButton(onClick = { vm.syncNow() }) { Text("Sync") }
                },
            )
        },
        floatingActionButton = {
            FloatingActionButton(onClick = onNew) {
                Text("+", style = MaterialTheme.typography.headlineSmall)
            }
        },
    ) { padding ->
        LazyColumn(
            modifier = Modifier.fillMaxSize().padding(padding),
            contentPadding = PaddingValues(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            item { SyncLine(status) }
            if (notes.isEmpty()) {
                item { Text("No notes yet. Tap + to write one. It works without a connection.") }
            }
            items(notes, key = { it.id }) { note ->
                NoteCard(note, onClick = { onOpen(note.id) })
            }
        }
    }
}

/** Create or edit one note. Saves go on top of the revision the editor opened, so a change made elsewhere isn't overwritten. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun EditorScreen(
    vm: NotesViewModel,
    id: String,
    onClose: () -> Unit,
) {
    val scope = rememberCoroutineScope()
    val conflicts by vm.conflicts(id).collectAsState(initial = emptyList())

    var loaded by rememberSaveable { mutableStateOf(false) }
    // The revision this editor started from. Null for a note that doesn't exist yet.
    var baseRev by rememberSaveable { mutableStateOf<String?>(null) }
    var title by rememberSaveable { mutableStateOf("") }
    var body by rememberSaveable { mutableStateOf("") }
    var pinned by rememberSaveable { mutableStateOf(false) }
    var message by remember { mutableStateOf<String?>(null) }

    fun show(note: Note?) {
        baseRev = note?.rev
        note?.let {
            title = it.title
            body = it.body
            pinned = it.pinned
        }
    }

    LaunchedEffect(id) {
        if (!loaded) {
            show(vm.load(id))
            loaded = true
        }
    }

    fun save() {
        scope.launch {
            // A new note left blank isn't worth keeping.
            if (baseRev == null && title.isBlank() && body.isBlank()) {
                onClose()
                return@launch
            }
            try {
                vm.save(id, baseRev, title.trim(), body, pinned)
                onClose()
            } catch (e: ConflictException) {
                message = "This note changed on another device while you were editing. Go back and open it again."
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                message = e.message ?: "Could not save"
            }
        }
    }

    fun delete() {
        val rev = baseRev ?: return
        scope.launch {
            try {
                vm.delete(id, rev)
                onClose()
            } catch (e: ConflictException) {
                message = "This note changed on another device. Go back and open it again."
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                message = e.message ?: "Could not delete"
            }
        }
    }

    fun resolve(chosenRev: String?) {
        scope.launch {
            try {
                vm.resolve(id, chosenRev)
                show(vm.load(id))
                message = null
            } catch (e: ConflictException) {
                message = "The note changed again. Try once more."
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                message = e.message ?: "Could not resolve the conflict"
            }
        }
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text(if (baseRev == null) "New note" else "Edit note") },
                navigationIcon = { TextButton(onClick = onClose) { Text("Back") } },
                actions = {
                    if (baseRev != null) TextButton(onClick = { delete() }) { Text("Delete") }
                    TextButton(onClick = { save() }) { Text("Save") }
                },
            )
        },
    ) { padding ->
        Column(
            modifier =
                Modifier
                    .fillMaxSize()
                    .padding(padding)
                    .verticalScroll(rememberScrollState())
                    .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            if (conflicts.isNotEmpty()) {
                ConflictPanel(
                    versions = conflicts,
                    onKeepMain = { resolve(null) },
                    onUse = { resolve(it) },
                )
            }
            OutlinedTextField(
                value = title,
                onValueChange = { title = it },
                label = { Text("Title") },
                singleLine = true,
                modifier = Modifier.fillMaxWidth(),
            )
            OutlinedTextField(
                value = body,
                onValueChange = { body = it },
                label = { Text("Note") },
                minLines = 6,
                modifier = Modifier.fillMaxWidth(),
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                Switch(checked = pinned, onCheckedChange = { pinned = it })
                Text("Pinned", modifier = Modifier.padding(start = 8.dp))
            }
            message?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        }
    }
}

@Composable
private fun SyncLine(status: SyncStatus) {
    val text =
        when {
            status.error != null -> "Sync stopped: ${status.error}"
            status.state is SyncState.Error -> "Can't reach the server. Will retry."
            status.state is SyncState.Pulling || status.state is SyncState.Pushing -> "Syncing…"
            status.lastSynced != null -> "Last synced ${DateFormat.getTimeInstance(DateFormat.SHORT).format(Date(status.lastSynced))}"
            else -> "Not synced yet."
        }
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        Text(text, style = MaterialTheme.typography.bodySmall)
        status.refused.forEach {
            Text(
                "Server refused: $it",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
            )
        }
    }
}

@Composable
private fun NoteCard(
    note: Note,
    onClick: () -> Unit,
) {
    Card(onClick = onClick, modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    note.title.ifBlank { "Untitled" },
                    style = MaterialTheme.typography.titleMedium,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(1f),
                )
                if (note.pinned) Text("Pinned", style = MaterialTheme.typography.labelSmall)
            }
            if (note.body.isNotBlank()) {
                Text(
                    note.body,
                    style = MaterialTheme.typography.bodyMedium,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                )
            }
        }
    }
}

/** Shown when another device saved a different version of this note. */
@Composable
private fun ConflictPanel(
    versions: List<Note>,
    onKeepMain: () -> Unit,
    onUse: (String) -> Unit,
) {
    Card(
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.errorContainer),
        modifier = Modifier.fillMaxWidth(),
    ) {
        Column(
            modifier = Modifier.padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            Text(
                "Another device saved a different version of this note. Pick the one to keep:",
                style = MaterialTheme.typography.bodyMedium,
            )
            TextButton(onClick = onKeepMain) { Text("Keep the main version (shown above)") }
            versions.forEach { other ->
                val time = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(Date(other.updatedAt))
                TextButton(onClick = { onUse(other.rev) }) {
                    Text("Use the other version from $time: ${other.title.ifBlank { "Untitled" }}")
                }
            }
        }
    }
}
