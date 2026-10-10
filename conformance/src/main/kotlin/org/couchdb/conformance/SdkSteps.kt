package org.couchdb.conformance

import com.thoughtworks.gauge.AfterScenario
import com.thoughtworks.gauge.Step
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.Credentials
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.couchdb.sync.Auth
import org.couchdb.sync.ConflictResolver
import org.couchdb.sync.CouchLite
import org.couchdb.sync.JdbcSqlDriver
import org.couchdb.sync.MemoryBlobStore
import org.couchdb.sync.Query
import org.couchdb.sync.RemoteDb
import org.couchdb.sync.Resolution
import org.couchdb.sync.SqlLocalStore
import org.couchdb.sync.SyncState
import java.util.UUID

/**
 * Gauge steps for the SDK's own behavior (`sdk-specs/`): devices are local
 * [CouchLite] databases, the server is a [RemoteDb] for the scenario.
 */
class SdkSteps {
    private val server = System.getenv("COUCHDB_URL") ?: "http://localhost:5984"
    private val user = System.getenv("COUCHDB_USER") ?: "admin"
    private val password = System.getenv("COUCHDB_PASSWORD") ?: "password"

    private var remote: RemoteDb? = null
    private val devices = mutableMapOf<String, CouchLite>()
    private val liveJobs = mutableMapOf<String, Job>()
    private val progress = mutableListOf<SyncState>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @AfterScenario
    fun cleanUp() =
        run {
            liveJobs.values.forEach { it.cancelAndJoin() }
            liveJobs.clear()
            devices.values.forEach { runCatching { it.close() } }
            devices.clear()
            remote?.let { runCatching { it.destroy() } }
            remote = null
            progress.clear()
        }

    private fun db(): RemoteDb = checkNotNull(remote) { "no sync database in this scenario" }

    private fun device(name: String): CouchLite = checkNotNull(devices[name]) { "no device $name" }

    // ---- setup ----

    @Step("Create a fresh sync database")
    fun createDatabase() =
        run {
            val r = RemoteDb("$server/sdk-${UUID.randomUUID().toString().take(8)}", Auth.Basic(user, password))
            r.ensureExists(create = true)
            remote = r
        }

    @Step("Create a sync device <name>")
    fun createDevice(name: String) = run { devices[name] = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore()) }

    @Step("Close device <name>")
    fun closeDevice(name: String) = run { device(name).close() }

    @Step("Device <name> fails to read document <id>")
    fun readClosed(
        name: String,
        id: String,
    ) = run {
        check(runCatching { device(name).get(id) }.isFailure) { "read from a closed device succeeded" }
    }

    // ---- documents and attachments ----

    @Step("Device <name> saves document <id> with field <field> = <value>")
    fun saveField(
        name: String,
        id: String,
        field: String,
        value: String,
    ) = run {
        val d = device(name)
        d.put(id, buildJsonObject { put(field, value) }, d.get(id)?.rev)
    }

    @Step("Device <name> saves document <id> with team <team> and score <score>")
    fun saveTeamScore(
        name: String,
        id: String,
        team: String,
        score: String,
    ) = run {
        val d = device(name)
        d.put(
            id,
            buildJsonObject {
                put("team", team)
                put("score", score.toInt())
            },
            d.get(id)?.rev,
        )
    }

    @Step("Device <name> saves document <id> with a <size>-byte attachment <file>")
    fun saveAttachment(
        name: String,
        id: String,
        size: String,
        file: String,
    ) = run {
        val d = device(name)
        val rev = d.get(id)?.rev ?: d.put(id, JsonObject(emptyMap()))
        d.putAttachment(id, rev, file, "application/octet-stream", payload(size.toInt()))
    }

    @Step("Device <name> has a <size>-byte attachment <file> on document <id>")
    fun deviceHasAttachment(
        name: String,
        size: String,
        file: String,
        id: String,
    ) = run {
        val bytes = device(name).getAttachment(id, file)
        check(bytes != null && bytes.contentEquals(payload(size.toInt()))) { "attachment $file is ${bytes?.size} bytes" }
    }

    @Step("Server attachment <file> of document <id> has <size> bytes")
    fun serverAttachmentSize(
        file: String,
        id: String,
        size: String,
    ) = run {
        val url = db().url.newBuilder().addPathSegment(id).addPathSegment(file).build()
        assertEq(size.toInt(), rawGet(url).size)
    }

    @Step("Device <name> has document <id> with field <field> = <value>")
    fun deviceHasField(
        name: String,
        id: String,
        field: String,
        value: String,
    ) = run {
        assertEq(value, fieldOf(device(name).get(id)!!.body, field))
    }

    @Step("Save document <id> on the server with field <field> = <value>")
    fun serverSave(
        id: String,
        field: String,
        value: String,
    ) = run {
        db().request("PUT", listOf(id), body = buildJsonObject { put(field, value) })
    }

    @Step("Server document <id> has field <field> = <value>")
    fun serverHasField(
        id: String,
        field: String,
        value: String,
    ) = run {
        assertEq(value, db().request("GET", listOf(id))!!.jsonObject[field]?.jsonPrimitive?.content)
    }

    @Step("Server has <n> documents named <prefix>")
    fun serverDocs(
        n: String,
        prefix: String,
    ) = run {
        repeat(n.toInt()) { i -> db().request("PUT", listOf("$prefix$i"), body = buildJsonObject { put("v", i) }) }
    }

    // ---- sync and conflicts ----

    @Step("Device <name> syncs with the server")
    fun sync(name: String) = run { device(name).replicator(db()).sync() }

    @Step("Device <name> syncs and resolves conflicts preferring field <field> = <value>")
    fun syncResolving(
        name: String,
        field: String,
        value: String,
    ) = run {
        device(name).replicator(db(), resolver = preferring(field, value)).sync()
    }

    @Step("Device <name> has <n> conflicts for document <id>")
    fun conflictCount(
        name: String,
        n: String,
        id: String,
    ) = run { assertEq(n.toInt(), device(name).conflicts(id).size) }

    @Step("Device <name> resolves document <id> by keeping field <field> = <value>")
    fun resolveByKeeping(
        name: String,
        id: String,
        field: String,
        value: String,
    ) = run {
        device(name).resolve(id, preferring(field, value))
    }

    @Step("Device <name> syncs in batches of <size> and records progress")
    fun syncInBatches(
        name: String,
        size: String,
    ) = run {
        val replicator = device(name).replicator(db(), batchSize = size.toInt())
        val watcher = scope.launch(Dispatchers.Unconfined) { replicator.state.collect { progress += it } }
        replicator.sync()
        watcher.cancelAndJoin()
    }

    @Step("Recorded progress includes pulling <docs> documents with <pending> pending")
    fun progressIncludes(
        docs: String,
        pending: String,
    ) = run {
        check(progress.contains(SyncState.Pulling(docs.toInt(), pending.toLong()))) { "progress was $progress" }
    }

    // ---- indexes ----

    @Step("Device <name> creates a compound index on <fields>")
    fun compoundIndex(
        name: String,
        fields: String,
    ) = run { device(name).createCompoundIndex(fields.split(",")) }

    @Step("Device <name> lists documents sorted by <fields> ascending as <ids>")
    fun listAscending(
        name: String,
        fields: String,
        ids: String,
    ) = listSorted(name, fields, true, ids)

    @Step("Device <name> lists documents sorted by <fields> descending as <ids>")
    fun listDescending(
        name: String,
        fields: String,
        ids: String,
    ) = listSorted(name, fields, false, ids)

    private fun listSorted(
        name: String,
        fields: String,
        ascending: Boolean,
        ids: String,
    ) = run {
        val sort = fields.split(",").map { it to ascending }
        val got = device(name).find(Query(JsonObject(emptyMap()), sort = sort)).map { it.id }
        assertEq(ids.split(","), got)
    }

    // ---- live sync ----

    @Step("Start continuous live sync for device <name>")
    fun startLive(name: String) =
        run {
            val d = device(name)
            val r = db()
            liveJobs[name] = scope.launch { d.liveSync(r, continuous = true) }
        }

    @Step("Stop live sync for device <name>")
    fun stopLive(name: String) = run { liveJobs.remove(name)?.cancelAndJoin() }

    @Step("Device <name> receives document <id> within <seconds> seconds")
    fun deviceReceives(
        name: String,
        id: String,
        seconds: String,
    ) = run {
        withTimeout(seconds.toLong() * 1000) { while (device(name).get(id) == null) delay(200) }
    }

    @Step("The server receives document <id> within <seconds> seconds")
    fun serverReceives(
        id: String,
        seconds: String,
    ) = run {
        withTimeout(seconds.toLong() * 1000) { while (db().request("GET", listOf(id)) == null) delay(200) }
    }

    // ---- helpers ----

    /** Keeps the winning version if it has [value] in [field]; otherwise the conflicting version that does. */
    private fun preferring(
        field: String,
        value: String,
    ) = ConflictResolver { winner, others ->
        when {
            fieldOf(winner.body, field) == value -> Resolution.KeepWinner
            else ->
                others.firstOrNull { fieldOf(it.body, field) == value }?.let { Resolution.Pick(it.rev) }
                    ?: Resolution.KeepWinner
        }
    }

    private fun fieldOf(
        body: JsonObject,
        field: String,
    ) = (body[field] as? JsonPrimitive)?.content

    private fun payload(size: Int) = ByteArray(size) { (it * 31 % 251).toByte() }

    private fun rawGet(url: HttpUrl): ByteArray {
        val req = Request.Builder().url(url).header("Authorization", Credentials.basic(user, password)).build()
        return OkHttpClient().newCall(req).execute().use { it.body!!.bytes() }
    }

    private fun assertEq(
        expected: Any?,
        actual: Any?,
    ) = check(expected == actual) { "expected $expected but was $actual" }

    private fun <T> run(block: suspend () -> T) {
        runBlocking { block() }
    }
}
