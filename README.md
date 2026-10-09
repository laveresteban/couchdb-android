# couchdb-android

Offline-first storage for Android apps with two-way sync to Apache CouchDB 3.x.
Reads and writes go to a local SQLite database; sync uses the standard CouchDB
replication protocol, so the server needs no plugins.

Design notes: [couchdb-sdk-generator/docs/android-offline-sync.md](https://github.com/laveresteban/couchdb-sdk-generator/blob/main/docs/android-offline-sync.md).

## Modules

| Module | What it is |
|--------|------------|
| `sync-core` | Plain Kotlin/JVM. Revision-tree store, CouchDB client, replicator, conflict handling. All the logic and tests live here. |
| `android` | Android glue: SQLite driver, `CouchLiteAndroid.open`, WorkManager `SyncWorker` and `SyncScheduler`. |

`android` is only included when an Android SDK is found (`ANDROID_HOME` or
`sdk.dir` in `local.properties`), so the core builds and tests on any JVM.

## Usage

```kotlin
// Application.onCreate
CouchLiteAndroid.remotes = { name ->
    RemoteDb("https://db.example.com/$name", Auth.Session(user, password))
}
val notes = CouchLiteAndroid.open(this, "notes")
SyncScheduler.schedulePeriodic(this, "notes")   // every 15 min when online

// Anywhere, online or not
val rev = notes.put("n1", buildJsonObject { put("title", "Groceries") })
notes.put("n1", buildJsonObject { put("title", "Groceries (2)") }, rev)
notes.get("n1")

// Sync now, or keep syncing while the screen is open
SyncScheduler.syncNow(this, "notes")
lifecycleScope.launch { notes.liveSync(CouchLiteAndroid.remotes!!("notes")) }

// Conflicts
if (notes.conflicts("n1").isNotEmpty()) {
    notes.resolve("n1") { winner, others -> Resolution.Merge(merge(winner, others)) }
}
```

## How sync works

- Every doc keeps its revision tree locally, like CouchDB. Local edits create
  child revisions; pulled revisions are inserted with their full history.
- The winning revision is picked with CouchDB's rule (not deleted > deleted,
  then higher generation, then higher hash), so the phone and the server
  always agree on the winner without talking to each other.
- **Pull:** `_changes?style=all_docs` → find missing revs locally →
  `_bulk_get?revs=true` → insert.
- **Push:** local changes since checkpoint → `_revs_diff` →
  `_bulk_docs` with `new_edits: false` and `_revisions`.
- Checkpoints are written to both sides (`_local/<id>` on the server). If they
  don't match, sync restarts from 0, which is safe because only missing
  revisions are copied.

## Tests

```sh
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3
./gradlew :sync-core:test
```

Replication tests are skipped if no CouchDB is reachable at `COUCHDB_URL`.

## Not done yet

- Attachments: pulled attachment stubs are kept and pushed back, but binary
  content isn't downloaded and new local attachments aren't supported.
- Local queries beyond `get` and `allDocs` (no indexes yet).
- Filtered sync (`_doc_ids` / `_selector`), compaction of old revision bodies,
  encryption at rest.
- The `android` module hasn't been run on a device yet; CI builds it.
- The Gauge conformance steps for Kotlin.

## Why not the generated client?

OpenAPI Generator's Kotlin output (kotlinx-serialization) models `Document`
as a `HashMap` subclass with fixed fields, which drops user fields. Sync needs
raw JSON, so `RemoteDb` is a small hand-written OkHttp client covering the
replication endpoints from couchdb-openapi.

## License

Apache License 2.0. See [LICENSE](LICENSE).
