# CLAUDE.md — couchdb-android

Offline-first CouchDB sync for Android. Design doc:
`couchdb-sdk-generator/docs/android-offline-sync.md`.

- `sync-core/`: plain Kotlin/JVM. `SqlLocalStore` (rev tree, compound indexes),
  `Replicator` (pull, push, live and continuous sync), `RemoteDb` (hand-written
  OkHttp client), `ChangesFeed`, `Mango`, `BlobStore` (streamed, encrypted).
  `src/testFixtures` holds `JdbcSqlDriver`, shared with `conformance/`.
- `android/`: SQLite/SQLCipher driver, Keystore secrets, `CouchLiteAndroid`,
  `SyncWorker`/`SyncScheduler`. Only built when an Android SDK is present.
- `conformance/`: Gauge Java/Kotlin steps. `src/` holds the steps for the shared
  specs (`specs/`, copied from couchdb-sdk-generator) and `SdkSteps.kt` for
  `sdk-specs/` (this repo's SDK behavior specs).
- `example-notes/`: Compose notes app on the SDK (rollout step 6 in the design).
- `interop/`: PouchDB (Node) for the protocol interop test.

No generated code: the Kotlin generator drops user fields on the open
`Document` schema (see generator README).

## Commands

```sh
docker run -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password couchdb:3.4
./gradlew ktlintCheck                              # formatting, all modules
./gradlew :sync-core:test :sync-core:koverVerify   # JVM tests, 88% line gate
./gradlew :android:testDebugUnitTest :android:koverVerify   # Robolectric, 65% gate
./gradlew :android:lintRelease :example-notes:lintDebug
./gradlew :android:connectedDebugAndroidTest       # emulator, CouchDB at 10.0.2.2
scripts/conformance.sh ../couchdb-sdk-generator    # Gauge: shared + sdk-specs
(cd interop && npm ci)                             # needed by PouchInteropTest
```

Gradle build cache, parallel builds and the configuration cache are on
(`gradle.properties`). Keep them on.

## Rules

- Winner selection must match CouchDB exactly (not deleted > deleted, then
  generation, then hash string). Don't change `Rev.compareTo` or
  `updateWinner` without a test against real server output.
- Store calls go through `io` dispatcher; `SqlLocalStore` serializes on `lock`.
- Every behavior change ships with tests. Core logic gets JVM tests; wire
  protocol gets `FakeCouch` tests (`ReplicatorProtocolTest`, `LiveProtocolTest`)
  and, where CouchDB semantics matter, a `ReplicationTest` case. SDK behavior
  visible to users also gets a Gauge scenario in `conformance/sdk-specs/`.
- Gauge parameters are quoted, numbers included (`has "2" documents`). Check
  new steps with `gauge run --simple-console --tags "..." specs sdk-specs`:
  `Step implementation not found` means a mismatch.
- Keep comments short. Document a public API with one line; don't restate code.
- Coverage gates only go up. Lower one only with a written reason in the PR.
- Before pushing a branch, run the Commands above locally and fix what fails.
  Push a feature branch, never `main`. Watch CI (`gh run watch`) and fix it
  until it is green before reporting the work done.
- The dev machine has no Docker/WSL, so CouchDB-backed tests run only in CI.
  Say so when reporting results that depend on them.

## Known gaps

- Pull fetches attachment bodies one at a time (no multipart `_bulk_get`).
- Arrays and objects in compound index keys sort by JSON text, not Mango order.
- `destroy` closes the open instance; code holding a `CouchLite` reference must
  call `open` again after it.
- Publishing: the Android javadoc jar is empty (Dokka can't read Kotlin sealed
  interfaces with AGP 8.7). Revisit when the toolchain updates.
