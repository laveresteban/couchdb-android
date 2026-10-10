package org.couchdb.sync

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Protocol interop with PouchDB (the same replication protocol, a different
 * implementation). Runs `interop/pouch.mjs` with Node against the same
 * CouchDB. Skipped unless Node, `interop/node_modules` and CouchDB are there.
 */
class PouchInteropTest {
    private val server = System.getenv("COUCHDB_URL") ?: "http://localhost:5984"
    private val user = System.getenv("COUCHDB_USER") ?: "admin"
    private val password = System.getenv("COUCHDB_PASSWORD") ?: "password"
    private val interop = File("../interop")
    private val dbName = "interop-${UUID.randomUUID().toString().take(8)}"
    private val dbUrl = "$server/$dbName"
    private val pouchUrl = "${server.replace("://", "://$user:$password@")}/$dbName"

    private fun remote() = RemoteDb(dbUrl, Auth.Basic(user, password))

    private fun device() = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore())

    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    @BeforeEach
    fun setUp() {
        val nodeOk = runCatching { ProcessBuilder("node", "--version").start().waitFor() == 0 }.getOrDefault(false)
        assumeTrue(nodeOk && File(interop, "node_modules").isDirectory, "needs node and interop/node_modules (npm ci)")
        val up =
            runCatching {
                OkHttpClient().newCall(Request.Builder().url("$server/_up").build()).execute().use { it.isSuccessful }
            }.getOrDefault(false)
        assumeTrue(up, "CouchDB not reachable at $server")
        runBlocking { remote().ensureExists(create = true) }
    }

    @AfterEach
    fun tearDown() {
        runBlocking { runCatching { remote().destroy() } }
    }

    /** Runs pouch.mjs with [args] and returns the JSON it printed last. */
    private fun pouch(vararg args: String): JsonObject {
        val process = ProcessBuilder(listOf("node", "pouch.mjs") + args).directory(interop).start()
        val out = process.inputStream.bufferedReader().readText()
        val err = process.errorStream.bufferedReader().readText()
        check(process.waitFor() == 0) { "pouch.mjs ${args.toList()} failed: $err" }
        return Json.parseToJsonElement(out.lines().last { it.isNotBlank() }).jsonObject
    }

    @Test
    fun `a document the Kotlin SDK pushes is read by PouchDB`() =
        runBlocking<Unit> {
            val phone = device()
            phone.put("from-kotlin", body("1"))
            phone.replicator(remote()).sync()
            assertEquals("1", pouch("read", pouchUrl, "from-kotlin")["v"]!!.jsonPrimitive.content)
        }

    @Test
    fun `a document PouchDB writes syncs into the Kotlin SDK and back`() =
        runBlocking<Unit> {
            pouch("write", pouchUrl, "from-pouch", "2")
            val phone = device()
            phone.replicator(remote()).sync()
            val doc = phone.get("from-pouch")!!
            assertEquals("2", doc.body["v"]!!.jsonPrimitive.content)

            phone.put("from-pouch", body("3"), doc.rev)
            phone.replicator(remote()).sync()
            assertEquals("3", pouch("read", pouchUrl, "from-pouch")["v"]!!.jsonPrimitive.content)
        }
}
