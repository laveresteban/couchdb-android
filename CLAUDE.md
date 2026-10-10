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
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3.4
./gradlew :sync-core:test
scripts/conformance.sh ../couchdb-sdk-generator
./gradlew :android:connectedDebugAndroidTest     # emulator, CouchDB at 10.0.2.2
```

## Rules

- Winner selection must match CouchDB exactly (not deleted > deleted, then
  generation, then hash string). Don't change `Rev.compareTo` or
  `updateWinner` without a test against real server output.
- Store calls go through `io` dispatcher; `SqlLocalStore` serializes on `lock`.

## Fixes still open

Done: refused pushes reported in `SyncResult.pushFailures` (no infinite
retry), only transient errors retried (`live`, `SyncWorker`), per-store
`syncLock`, cached `_local` revs, batched continuous checkpoints saved on
`stop()`, `pullErrors` count, missing attachment blobs reported, pushes split
by `maxBatchBytes`, `open()` encryption mismatch throws, read timeout follows
longpoll/heartbeat, single login per expiry, `onChange` after commit, CI pins.

- `SyncWorker` failures only carry a count of push failures; apps that need
  details should call `replicator().sync()` themselves.

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
