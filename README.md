# couchdb-android

Offline-first storage for Android apps with two-way sync to Apache CouchDB 3.x.
Reads and writes go to a local SQLite database; sync uses the standard CouchDB
replication protocol, so the server needs no plugins.

Design notes: [couchdb-sdk-generator/docs/android-offline-sync.md](https://github.com/laveresteban/couchdb-sdk-generator/blob/main/docs/android-offline-sync.md).

## Modules

| Module | What it is |
|--------|------------|
| `sync-core` | Plain Kotlin/JVM. Revision-tree store, Mango queries and indexes, attachments, CouchDB client, replicator, conflict handling. Most logic and tests live here. |
| `android` | Android glue: SQLite/SQLCipher driver, Keystore-protected keys, `CouchLiteAndroid.open`, WorkManager `SyncWorker` and `SyncScheduler`. |
| `conformance` | Kotlin steps for the shared Gauge specs in couchdb-sdk-generator. |

`android` is only included when an Android SDK is found (`ANDROID_HOME` or
`sdk.dir` in `local.properties`), so the rest builds and tests on any JVM.

## Usage

```kotlin
// Application.onCreate
CouchLiteAndroid.remotes = { name ->
    RemoteDb("https://db.example.com/$name", Auth.Session(user, password))
}
val notes = CouchLiteAndroid.open(this, "notes", encrypted = true)
SyncScheduler.schedulePeriodic(this, "notes", encrypted = true)   // every 15 min when online

// Reads and writes work offline
val rev = notes.put("n1", buildJsonObject { put("title", "Groceries"); put("pinned", true) })
notes.putAttachment("n1", rev, "list.jpg", "image/jpeg", jpegBytes)
notes.getAttachment("n1", "list.jpg")

// Queries use Mango selectors, same as CouchDB _find
notes.createIndex("pinned")
val pinned = Query(buildJsonObject { put("pinned", true) }, sort = listOf("title" to true))
notes.observe(pinned).collect { render(it) }   // re-runs on local edits and synced changes

// Sync now, or keep syncing while the screen is open
SyncScheduler.syncNow(this, "notes", encrypted = true)
lifecycleScope.launch { notes.liveSync(CouchLiteAndroid.remotes!!("notes")) }

// Sync only part of the database
val mine = SyncFilter.Selector(buildJsonObject { put("owner", userId) })
notes.replicator(remote, filter = mine).sync()

// Conflicts
if (notes.conflicts("n1").isNotEmpty()) {
    notes.resolve("n1") { winner, others -> Resolution.Merge(merge(winner, others)) }
}

// Housekeeping: drop old revision bodies and unused attachment files
notes.compact(revsLimit = 100)
```

## How sync works

- Every doc keeps its revision tree locally, like CouchDB. Local edits create
  child revisions; pulled revisions are inserted with their full history.
- The winning revision is picked with CouchDB's rule (not deleted > deleted,
  then higher generation, then higher hash), so the phone and the server
  always agree on the winner without talking to each other.
- **Pull:** `_changes?style=all_docs` → find missing revs locally →
  `_bulk_get?revs=true&attachments=true` with `atts_since` set to the revs
  we have, so only new attachment content is downloaded.
- **Push:** local changes since checkpoint → `_revs_diff` →
  `_bulk_docs` with `new_edits: false` and `_revisions`. Attachments the
  server already has (per `possible_ancestors`) go as stubs, the rest inline.
- **Filters:** pull uses the server's `_doc_ids` / `_selector` filter, push
  checks the same filter locally. Each filter gets its own checkpoint.
- Checkpoints are written to both sides (`_local/<id>` on the server). If they
  don't match, sync restarts from 0, which is safe because only missing
  revisions are copied.

## Queries

`find` and `observe` take Mango selectors: `$eq $ne $gt $gte $lt $lte $in
$nin $exists $type $size $all $elemMatch $regex $mod`, `$and $or $nor $not`,
dotted paths and nested objects. Comparisons follow CouchDB collation, so
ranges cross types the same way the server does (`{"age": {"$gte": 30}}`
also matches string ages). Indexes are optional and single-field; a query
uses one when it has a top-level condition on an indexed field.

## Encryption

With `encrypted = true` the database file uses SQLCipher and attachment files
use AES-256-GCM. Both use a random 32-byte secret per database, stored
encrypted by a key in the Android Keystore. If that key is lost (for example,
app data cleared), call `CouchLiteAndroid.destroy` and sync again.
Attachments are kept under `noBackupFilesDir`.

## Tests

```sh
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3
./gradlew :sync-core:test                       # unit + replication tests
scripts/conformance.sh ../couchdb-sdk-generator  # Gauge specs (needs gauge + java plugin)
./gradlew :android:connectedDebugAndroidTest     # on a device/emulator, CouchDB at 10.0.2.2
```

Replication tests are skipped if no CouchDB is reachable at `COUCHDB_URL`.
The conformance module runs the `documents`, `changes`, `sync` and
`databases` specs.

## Limits

- Attachments are held in memory while syncing (inline base64), so keep them
  to a few MB each. Multipart streaming isn't implemented.
- Indexes are single-field. `$or`/`$nor` queries and sort run in memory.
- Live sync uses longpoll, not the continuous feed.

## Why not the generated client?

OpenAPI Generator's Kotlin output (kotlinx-serialization) models `Document`
as a `HashMap` subclass with fixed fields, which drops user fields. Sync needs
raw JSON, so `RemoteDb` is a small hand-written OkHttp client covering the
replication endpoints from couchdb-openapi.

## License

Apache License 2.0. See [LICENSE](LICENSE).
