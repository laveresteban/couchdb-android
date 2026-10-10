# CLAUDE.md — couchdb-android

Offline-first CouchDB sync for Android. Design doc:
`couchdb-sdk-generator/docs/android-offline-sync.md`.

- `sync-core/`: plain Kotlin/JVM. `SqlLocalStore` (rev tree), `Replicator`,
  `RemoteDb` (hand-written OkHttp client), `ChangesFeed`, `Mango`, `BlobStore`.
  Most logic and tests live here.
- `android/`: SQLite/SQLCipher driver, Keystore secrets, `CouchLiteAndroid`,
  `SyncWorker`/`SyncScheduler`. Only built when an Android SDK is present.
- `conformance/`: Gauge Java/Kotlin steps. Runs `documents | changes | sync | databases`.

No generated code: the Kotlin generator drops user fields on the open
`Document` schema (see generator README).

## Commands

```sh
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3
./gradlew :sync-core:test
scripts/conformance.sh ../couchdb-sdk-generator
./gradlew :android:connectedDebugAndroidTest     # emulator, CouchDB at 10.0.2.2
```

## Rules

- Winner selection must match CouchDB exactly (not deleted > deleted, then
  generation, then hash string). Don't change `Rev.compareTo` or
  `updateWinner` without a test against real server output.
- Store calls go through `io` dispatcher; `SqlLocalStore` serializes on `lock`.

## Fixes

1. **Permanent push errors loop forever.** `bulkDocsNoNewEdits` turns any
   per-doc error (e.g. `forbidden` from `validate_doc_update`) into
   `CouchException(500, ...)`. That's an `IOException`, so `Replicator.live`
   and `SyncWorker` retry it forever, and every later change is stuck behind
   it. Keep the real error, skip/report rejected docs (CouchDB does this as
   `doc_write_failures`), and advance the checkpoint.
2. **`Replicator.live` retries all `IOException`s**, including 400/403/404.
   Use `ChangesFeed.isTransient` and stop on permanent errors. Same in
   `SyncWorker` (return `failure()` instead of `retry()`).
3. **Concurrent replicators on the same db race.** `SyncWorker` and
   `liveSync` can run at once; both GET then PUT the same `_local` checkpoint,
   so one gets 409 and retries. Hold a per-database `Mutex` (or a WorkManager
   unique name shared with live sync).
4. **Checkpoint writes cost two requests each.** `writeCheckpoint` and
   `ChangesFeed.save` GET the `_local` doc to learn its `_rev` before every
   PUT. Cache the `_rev` from the last PUT. `ChangesFeed` continuous mode
   does this per change; batch like Python (every N changes or on idle).
5. **Pulled revisions that fail `_bulk_get` are dropped.** Error entries
   (`not_found`, etc.) are ignored and the checkpoint moves past them. Log
   them and expose the count in `SyncResult`.
6. **Push sends stubs for attachments it doesn't have.** In `toWireDoc`, if
   the blob is missing locally the stub goes out unchanged; the server rejects
   it if it doesn't have that digest. Detect and skip or fail clearly.
7. **Large pushes go out as one body.** 100 docs × inline base64 attachments
   can exceed memory or server `max_http_request_size` (413). Split batches
   by byte size, not just doc count.
8. **`CouchLiteAndroid.open` cache ignores `encrypted`.** Opening the same
   name with a different flag returns the cached instance silently. Throw.
9. `RemoteDb` read timeout is fixed at 90 s; a `longpollMs` above ~80 s
   times out. Derive it from the longpoll/heartbeat value.
10. `AuthInterceptor.login` isn't synchronized; parallel requests after
    expiry all log in at once. Guard with a lock and re-check the cookie.
11. `SqlLocalStore.onChange` fires inside the transaction, before commit.
    If the transaction rolls back (e.g. `resolve` conflict), observers see a
    change that didn't happen. Fire after commit.
12. CI uses `couchdb:3` while the other repos use `couchdb:3.4`, and
    `getgauge/setup-gauge@master` is unpinned. Pin both.

## Features to add

- `Resolution.Pick(rev)` (in the design, not implemented) and an optional
  resolver that runs automatically after pull.
- Multipart attachment streaming (`_bulk_get` multipart, `PUT` with
  `follows`) to lift the "few MB" limit.
- `liveSync` on the continuous feed instead of longpoll.
- Sync state per direction with pending counts (`pending` from `_changes`).
- Compound (multi-field) indexes; use the index for sort.
- Sample notes app that works in airplane mode (rollout step 6 in the design).
- PouchDB interop test (from the design's testing plan).
- Run more conformance tags (`attachments`, `auth`) once `RemoteDb` covers them,
  or use the generated Kotlin client for non-document endpoints.
- Publish to Maven Central (no release workflow yet).
