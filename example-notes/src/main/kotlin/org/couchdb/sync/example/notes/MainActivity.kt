package org.couchdb.sync.example.notes

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LifecycleEventEffect
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            NotesTheme {
                NotesRoot()
            }
        }
    }
}

/** Picks the screen: setup until a server is set up, then the list or an editor. */
@Composable
private fun NotesRoot() {
    val vm: NotesViewModel = viewModel()
    val configured by vm.configured.collectAsStateWithLifecycle()
    var editingServer by rememberSaveable { mutableStateOf(false) }
    var editingNote by rememberSaveable { mutableStateOf<String?>(null) }

    // Live sync runs while the app is on screen. When it leaves, WorkManager takes over.
    LifecycleEventEffect(Lifecycle.Event.ON_START) { vm.startLiveSync() }
    LifecycleEventEffect(Lifecycle.Event.ON_STOP) {
        vm.stopLiveSync()
        vm.syncInBackground()
    }

    val noteId = editingNote
    when {
        !configured || editingServer -> SetupScreen(vm, onDone = { editingServer = false })
        noteId != null -> EditorScreen(vm, noteId, onClose = { editingNote = null })
        else ->
            NotesScreen(
                vm = vm,
                onOpen = { editingNote = it },
                onNew = { editingNote = vm.newNoteId() },
                onServer = { editingServer = true },
            )
    }
}

@Composable
private fun NotesTheme(content: @Composable () -> Unit) {
    val colors = if (isSystemInDarkTheme()) darkColorScheme() else lightColorScheme()
    MaterialTheme(colorScheme = colors, content = content)
}
