package org.couchdb.sync

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.put
import okhttp3.Credentials
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Syncs against a real CouchDB at COUCHDB_URL (admin/password by default).
 * Skipped when no server is reachable.
 */
class ReplicationTest {
    private val server = System.getenv("COUCHDB_URL") ?: "http://localhost:5984"
    private val user = System.getenv("COUCHDB_USER") ?: "admin"
    private val password = System.getenv("COUCHDB_PASSWORD") ?: "password"
    private val http = OkHttpClient()
    private val dbName = "sync-test-${UUID.randomUUID().toString().take(8)}"
    private val dbUrl = "$server/$dbName"

    private fun remote(auth: Auth = Auth.Basic(user, password)) = RemoteDb(dbUrl, auth)
    private fun device() = CouchLite(SqlLocalStore(JdbcSqlDriver()))
    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    @BeforeEach
    fun setUp() {
        val up = runCatching { http.newCall(Request.Builder().url("$server/_up").build()).execute().use { it.isSuccessful } }
        assumeTrue(up.getOrDefault(false), "CouchDB not reachable at $server")
        rest("PUT", "")
    }

    @AfterEach
    fun tearDown() {
        runCatching { rest("DELETE", "") }
    }

    @Test
    fun `offline writes are pushed`() = runBlocking {
        val phone = device()
        phone.put("a", body("1"))
        phone.put("b", body("1"))
        assertEquals(SyncResult(pulled = 0, pushed = 2), phone.replicator(remote()).sync())
        assertEquals("1", rest("GET", "/a")["v"]!!.jsonPrimitive.content)
    }

    @Test
    fun `server edits are pulled`() = runBlocking {
        rest("PUT", "/a", buildJsonObject { put("v", "server") })
        val phone = device()
        phone.replicator(remote()).sync()
        assertEquals("server", phone.get("a")!!.body["v"]!!.jsonPrimitive.content)
    }

    @Test
    fun `two devices converge through the server`() = runBlocking {
        val p1 = device()
        val p2 = device()
        val rev = p1.put("note", body("first"))
        p1.replicator(remote()).sync()
        p2.replicator(remote()).sync()
        val doc = p2.get("note")!!
        assertEquals(rev, doc.rev)
        p2.put("note", body("edited"), doc.rev)
        p2.replicator(remote()).sync()
        p1.replicator(remote()).sync()
        assertEquals("edited", p1.get("note")!!.body["v"]!!.jsonPrimitive.content)
    }

    @Test
    fun `concurrent edits conflict and every replica picks the same winner`() = runBlocking {
        val p1 = device()
        val p2 = device()
        val rev = p1.put("n", body("base"))
        p1.replicator(remote()).sync()
        p2.replicator(remote()).sync()
        p1.put("n", body("p1"), rev)
        p2.put("n", body("p2"), rev)
        p1.replicator(remote()).sync()
        p2.replicator(remote()).sync()
        p1.replicator(remote()).sync()

        val serverWinner = rest("GET", "/n")["_rev"]!!.jsonPrimitive.content
        assertEquals(serverWinner, p1.get("n")!!.rev)
        assertEquals(serverWinner, p2.get("n")!!.rev)
        assertEquals(1, p1.conflicts("n").size)

        p1.resolve("n") { _, _ -> Resolution.Merge(body("merged")) }
        p1.replicator(remote()).sync()
        p2.replicator(remote()).sync()
        assertEquals("merged", p2.get("n")!!.body["v"]!!.jsonPrimitive.content)
        assertEquals(0, p2.conflicts("n").size)
        val server = rest("GET", "/n?conflicts=true")
        assertNull(server["_conflicts"])
    }

    @Test
    fun `deletes propagate both ways`() = runBlocking {
        val phone = device()
        val rev = phone.put("a", body("1"))
        phone.replicator(remote()).sync()
        phone.delete("a", rev)
        phone.replicator(remote()).sync()
        assertEquals(404, status("GET", "/a"))

        rest("PUT", "/b", buildJsonObject { put("v", "1") })
        phone.replicator(remote()).sync()
        val serverRev = rest("GET", "/b")["_rev"]!!.jsonPrimitive.content
        rest("DELETE", "/b?rev=$serverRev")
        phone.replicator(remote()).sync()
        assertNull(phone.get("b"))
    }

    @Test
    fun `checkpoints let the next sync skip finished work`() = runBlocking {
        repeat(30) { rest("PUT", "/d$it", buildJsonObject { put("v", it) }) }
        val phone = device()
        val rep = phone.replicator(remote(), batchSize = 7)
        assertEquals(30, rep.sync().pulled)
        assertEquals(SyncResult(0, 0), rep.sync())
        assertEquals(30, phone.allDocs().size)
    }

    @Test
    fun `cookie session auth works`() = runBlocking {
        val phone = device()
        phone.put("a", body("1"))
        assertEquals(1, phone.replicator(remote(Auth.Session(user, password))).sync().pushed)
    }

    @Test
    fun `wrong password reports auth required`() {
        val e = runCatching { runBlocking { device().replicator(remote(Auth.Basic(user, "nope"))).sync() } }
        assert(e.exceptionOrNull() is AuthRequiredException) { "got ${e.exceptionOrNull()}" }
    }

    private fun status(method: String, path: String): Int =
        http.newCall(request(method, path, null)).execute().use { it.code }

    private fun rest(method: String, path: String, json: JsonObject? = null): JsonObject =
        http.newCall(request(method, path, json)).execute().use {
            val text = it.body!!.string()
            check(it.isSuccessful) { "$method $path: ${it.code} $text" }
            Json.parseToJsonElement(text).jsonObject
        }

    private fun request(method: String, path: String, json: JsonObject?) = Request.Builder()
        .url("$dbUrl$path")
        .header("Authorization", Credentials.basic(user, password))
        .method(method, json?.toString()?.toRequestBody("application/json".toMediaType()) ?: if (method == "PUT") ByteArray(0).toRequestBody() else null)
        .build()
}
