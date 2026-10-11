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
| `example-notes` | A notes app that works offline and syncs. Start here: [example-notes/README.md](example-notes/README.md). |

`android` and `example-notes` are only included when an Android SDK is found
(`ANDROID_HOME` or `sdk.dir` in `local.properties`), so the rest builds and
tests on any JVM.

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

// Revisions the server refused (e.g. validate_doc_update) are reported, not retried forever
val result = notes.replicator(remote).sync()
result.pushFailures.forEach { log("not synced: ${it.id} ${it.error} ${it.reason}") }

// Sync only part of the database
val mine = SyncFilter.Selector(buildJsonObject { put("owner", userId) })
notes.replicator(remote, filter = mine).sync()

// Conflicts: watch them, settle them by hand, or let the replicator settle them after each pull
notes.observeConflicts("n1").collect { losers -> if (losers.isNotEmpty()) showChoices(losers) }
notes.resolve("n1") { winner, others -> Resolution.Pick(others.first().rev) }   // or KeepWinner, Merge(body)
notes.replicator(remote, resolver = ConflictResolver { _, _ -> Resolution.KeepWinner }).sync()

// Housekeeping: drop old revision bodies and unused attachment files
notes.compact(revsLimit = 100)
notes.close()                                    // later open() calls reopen it
```

## Following the changes feed

For apps that react to server changes without keeping a local copy:

```kotlin
val feed = ChangesFeed(remote, checkpoint = "notifier", continuous = true)
feed.asFlow().collect { change -> notify(change.id) }   // call feed.stop() to end
```

With a `checkpoint` name, the position is saved in `_local/<name>` on the
server (after each longpoll batch; every `batchSize` changes and on `stop()`
for continuous), so a restarted reader resumes where the last one stopped
(at-least-once). Network errors, 429 and 5xx are retried with
jittered backoff that honors `Retry-After`. Longpoll is the default;
`continuous = true` keeps one streaming connection open with heartbeats.

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
  `_bulk_docs` with `new_edits: false` and `_revisions`, split so no request
  body exceeds `maxBatchBytes` (4 MB). Attachments the server already has (per
  `possible_ancestors`) go as stubs, the rest inline. Revisions the server
  refuses are returned in `SyncResult.pushFailures` and skipped, like
  CouchDB's own `doc_write_failures`.
- **Errors:** network errors, 429 and 5xx are retried with backoff (live sync
  and `SyncWorker`); 401 and other 4xx stop sync. Syncs of the same database
  take turns, so a background sync and live sync can't race.
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
Attachments are kept under `noBackupFilesDir`. Attachments over 1 MB stream as multipart
parts instead of inline base64. Attachments over 1 MB stream as multipart
parts instead of inline base64.

## Tests

```sh
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3.5.2
./gradlew :sync-core:test                       # unit tests; CouchDB and PouchDB tests when reachable
./gradlew :sync-core:koverVerify                 # line coverage gate (88%)
./gradlew :android:testDebugUnitTest             # Robolectric: SQLite driver, lifecycle, workers, scheduling
./gradlew :android:connectedDebugAndroidTest     # on a device/emulator, CouchDB at 10.0.2.2
scripts/conformance.sh ../couchdb-sdk-generator  # Gauge: shared specs + conformance/sdk-specs (needs gauge + java plugin)
(cd interop && npm ci)                           # PouchDB, for the interop test
./gradlew ktlintCheck                            # formatting, all modules
```

Replication tests are skipped if no CouchDB is reachable at `COUCHDB_URL`.
The conformance module runs the `documents`, `changes`, `sync` and
`databases` specs, and fails if any scenario is skipped. With Docker only:
`docker compose run --rm gauge-android` from couchdb-sdk-generator.

## Limits

- Attachments up to 1 MB go inline; larger ones stream as multipart parts. Pulls fetch
  attachment bodies one at a time.
- Indexes are single-field or compound (a sort on a prefix of a compound index reads it).
  `$or`/`$nor` queries and other sorts run in memory.
- `liveSync(continuous = true)` keeps one streaming connection open; the default longpolls.

## Publishing

Maven Central coordinates: `io.github.laveresteban:sync-core` and
`io.github.laveresteban:couchdb-android`. Pushing a `v*` tag runs
`.github/workflows/release.yml`, which needs the `MAVEN_CENTRAL_USERNAME`,
`MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY` and `SIGNING_PASSWORD` secrets.

## Why not the generated client?

OpenAPI Generator's Kotlin output (kotlinx-serialization) models `Document`
as a `HashMap` subclass with fixed fields, which drops user fields. Sync needs
raw JSON, so `RemoteDb` is a small hand-written OkHttp client covering the
replication endpoints from couchdb-openapi.

## License

Apache License 2.0. See [LICENSE](LICENSE).
