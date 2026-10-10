# Notes example

A small notes app that works offline and syncs with CouchDB 3.x. It shows the
SDK end to end: local writes, live and background sync, conflicts, and
encrypted storage. Read it alongside the [main README](../README.md).

## Run it

1. Start CouchDB:

   ```sh
   docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3.4
   ```

2. Install the app on an emulator or a connected device (needs the Android SDK,
   via `ANDROID_HOME` or `sdk.dir` in `local.properties`):

   ```sh
   ./gradlew :example-notes:installDebug
   ```

3. Open Notes and fill in the setup screen:

   | Where the app runs | Server URL | User / password |
   |--------------------|------------|-----------------|
   | Emulator | `http://10.0.2.2:5984` | `admin` / `password` |
   | Physical device | `http://<this computer's LAN IP>:5984` | `admin` / `password` |

   `10.0.2.2` is how an emulator reaches the computer it runs on. The setup
   screen checks the server and login before saving anything, and creates the
   `notes` database if it doesn't exist.

## Try it

- **Offline:** create a note, then turn on airplane mode. You can still create
  and edit notes.
- **Sync:** turn airplane mode off. The status line shows "Syncing…" and the
  note appears in CouchDB (Fauxton at `http://localhost:5984/_utils`, database
  `notes`).
- **Conflicts:** edit the same note on two devices while both are offline.
  When they reconnect, the editor shows the other version and lets you choose
  which one to keep.

## Where to look

| File | What it shows |
|------|---------------|
| [`NotesApp.kt`](src/main/kotlin/org/couchdb/sync/example/notes/NotesApp.kt) | Setting `CouchLiteAndroid.remotes` for background sync, and scheduling periodic sync. |
| [`Settings.kt`](src/main/kotlin/org/couchdb/sync/example/notes/Settings.kt) | Server settings encrypted with `EncryptedSharedPreferences`, and building a `RemoteDb`. |
| [`Note.kt`](src/main/kotlin/org/couchdb/sync/example/notes/Note.kt) | How a note is stored: plain JSON with `"type": "note"`, mapped from `Doc`. |
| [`NotesViewModel.kt`](src/main/kotlin/org/couchdb/sync/example/notes/NotesViewModel.kt) | `open`, `observe` for the list, `put` and `delete` with revisions, `resolve`, live and manual sync, `SyncScheduler`. |
| [`Screens.kt`](src/main/kotlin/org/couchdb/sync/example/notes/Screens.kt) | The editor's save-on-base-revision logic, the conflict panel, and refused changes in the status line. |

## Before you ship something like this

- Use HTTPS, and give the app its own CouchDB user with access only to its
  database. Don't ship admin credentials. The example uses cleartext HTTP only
  in debug builds (`src/debug/AndroidManifest.xml`).
- Connecting creates the database if it's missing. If your server creates
  databases for you, change `ensureExists(create = true)` in
  `NotesViewModel.connect`.
- Backups are off (`android:allowBackup="false"`). The local database is
  readable only with this device's Keystore key, so a restored copy can't be
  opened; the app syncs it again from the server instead.
- To sync only some notes, pass a filter to `db.replicator(...)`, for example
  `SyncFilter.Selector(buildJsonObject { put("type", "note") })`. The example
  syncs everything in the database.
