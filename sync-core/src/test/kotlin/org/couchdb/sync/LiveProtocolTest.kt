package org.couchdb.sync

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Live, continuous and session behavior against [FakeCouch]. */
class LiveProtocolTest {
    private val server = okhttp3.mockwebserver.MockWebServer()
    private val couch = FakeCouch()
    private val url by lazy { server.url("/db").toString() }

    @BeforeEach
    fun start() {
        server.dispatcher = couch
        server.start()
        couch.on("GET", "/db") { FakeCouch.json("{}") }
        couch.withEmptyCheckpoints()
        couch.on("GET", "/db/_changes") { FakeCouch.json("""{"results":[],"last_seq":"0"}""") }
        couch.on("POST", "/db/_revs_diff") { FakeCouch.json("{}") }
    }

    @AfterEach
    fun stop() {
        server.shutdown()
    }

    private fun remote() = RemoteDb(url)

    private fun continuousRequests() = couch.requests.filter { it.requestUrl!!.queryParameter("feed") == "continuous" }

    @Test
    fun `a session logs in, then logs in again once its cookie expires`() =
        runBlocking<Unit> {
            couch.on("POST", "/_session") { _ ->
                val name = if (couch.requests.count { it.path == "/_session" } > 1) "fresh" else "first"
                MockResponse()
                    .setResponseCode(200)
                    .setHeader("Set-Cookie", "AuthSession=$name; Path=/; HttpOnly")
                    .setHeader("Content-Type", "application/json")
                    .setBody("""{"ok":true}""")
            }
            couch.on("GET", "/db") { request ->
                val cookie = request.getHeader("Cookie")
                if (cookie == "AuthSession=first" || cookie == "AuthSession=fresh") FakeCouch.json("{}") else FakeCouch.json("{}", 401)
            }
            val db = RemoteDb(url, Auth.Session("admin", "password"))
            db.ensureExists(create = false)
            db.ensureExists(create = false)
            assertEquals(1, couch.recorded("POST", "/_session").size)

            // The server forgets the first cookie: the next request sees a 401 and logs in again.
            couch.on("GET", "/db") { request ->
                if (request.getHeader("Cookie") == "AuthSession=fresh") FakeCouch.json("{}") else FakeCouch.json("{}", 401)
            }
            db.ensureExists(create = false)
            assertEquals(2, couch.recorded("POST", "/_session").size)
        }

    @Test
    fun `a continuous feed yields each change line`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") {
                MockResponse().setHeader("Content-Type", "application/json").setBody(
                    """{"seq":"1","id":"a","changes":[{"rev":"1-a"}]}""" + "\n\n" +
                        """{"seq":"2","id":"b","changes":[{"rev":"1-b"}]}""" + "\n",
                )
            }
            val changes = remote().continuousChanges("0").toList()
            assertEquals(listOf("a", "b"), changes.map { it.id })
        }

    @Test
    fun `a continuous feed that fails with a server error is reported`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") { FakeCouch.json("""{"error":"boom"}""", 500) }
            val failed = runCatching { remote().continuousChanges("0").toList() }.exceptionOrNull()
            assertTrue(failed is CouchException && failed.status == 500, "$failed")
        }

    @Test
    fun `the change feed checkpoints what it delivered`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") {
                FakeCouch.json("""{"results":[{"seq":"5","id":"a","changes":[{"rev":"1-a"}]}],"last_seq":"5"}""")
            }
            val feed = ChangesFeed(remote(), checkpoint = "reader")
            feed.asFlow().collect { feed.stop() }
            assertEquals(1, couch.requests.count { it.method == "PUT" && it.requestUrl!!.encodedPath.startsWith("/db/_local/") })
        }

    @Test
    fun `longpoll live sync pulls remote changes and pushes local ones`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") {
                FakeCouch.json("""{"results":[],"last_seq":"0"}""").setBodyDelay(50, TimeUnit.MILLISECONDS)
            }
            val phone = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore())
            phone.put("local", JsonObject(mapOf("v" to JsonPrimitive("1"))))
            val writes = MutableSharedFlow<Unit>()
            withTimeoutOrNull(1_500) {
                phone.replicator(remote()).live(writes, longpollMs = 100, debounceMs = 0)
            }
            assertTrue(couch.recorded("GET", "/db/_changes").size >= 2)
            assertTrue(couch.recorded("POST", "/db/_revs_diff").isNotEmpty())
        }

    @Test
    fun `continuous live sync reconnects after the stream ends and syncs on each signal`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") { request ->
                if (request.requestUrl!!.queryParameter("feed") == "continuous") {
                    MockResponse().setBody("""{"seq":"1","id":"x","changes":[{"rev":"1-x"}]}""" + "\n")
                } else {
                    FakeCouch.json("""{"results":[],"last_seq":"0"}""")
                }
            }
            val phone = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore())
            withTimeoutOrNull(2_500) {
                phone.replicator(remote()).live(MutableSharedFlow(), continuous = true, debounceMs = 0)
            }
            assertTrue(continuousRequests().size >= 2, "continuous connections: ${continuousRequests().size}")
        }

    @Test
    fun `continuous live sync keeps retrying through server errors`() =
        runBlocking<Unit> {
            couch.on("GET", "/db/_changes") { request ->
                if (request.requestUrl!!.queryParameter("feed") == "continuous") {
                    FakeCouch.json("""{"error":"busy"}""", 503)
                } else {
                    FakeCouch.json("""{"results":[],"last_seq":"0"}""")
                }
            }
            val phone = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore())
            withTimeoutOrNull(2_500) {
                phone.replicator(remote()).live(MutableSharedFlow(), continuous = true, debounceMs = 0)
            }
            assertTrue(continuousRequests().size >= 2, "connections: ${continuousRequests().size}")
        }
}
