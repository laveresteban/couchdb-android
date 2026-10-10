package org.couchdb.sync

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class RemoteDbTest {
    private val server = MockWebServer()
    private val couch = FakeCouch()
    private val url by lazy { server.url("/db").toString() }

    @BeforeEach
    fun start() {
        server.dispatcher = couch
        server.start()
    }

    @AfterEach
    fun stop() {
        server.shutdown()
    }

    private fun attachment() = PendingAttachment("a.bin", "application/octet-stream", 4, attachmentDigest("abcd".toByteArray()))

    @Test
    fun `reads an attachment's bytes at the given revision`() =
        runBlocking {
            couch.on("GET", "/db/doc/a.bin") { MockResponse().setBody(Buffer().write("abcd".toByteArray())) }
            val bytes = RemoteDb(url).readAttachment("doc", "a.bin", "1-a") { it.readBytes() }
            assertContentEquals("abcd".toByteArray(), bytes)
            assertEquals("1-a", couch.recorded("GET", "/db/doc/a.bin").single().requestUrl!!.queryParameter("rev"))
        }

    @Test
    fun `a missing attachment is a CouchException with the server's status`() =
        runBlocking {
            couch.on("GET", "/db/doc/gone") { FakeCouch.json("""{"error":"not_found","reason":"missing"}""", 404) }
            assertEquals(
                404,
                assertFailsWith<CouchException> { RemoteDb(url).readAttachment("doc", "gone", "1-a") { it.readBytes() } }.status,
            )
        }

    @Test
    fun `a refused multipart write is returned as a failure`() =
        runBlocking {
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"error":"forbidden","reason":"no"}""", 403) }
            val failure =
                RemoteDb(url).putWithAttachments("doc", JsonObject(mapOf("_rev" to JsonPrimitive("2-b"))), listOf(attachment())) {
                    "abcd".toByteArray().inputStream()
                }
            assertEquals(WriteFailure("doc", "2-b", "forbidden", "no"), failure)
        }

    @Test
    fun `auth and server errors on a multipart write are thrown so the caller can retry`() =
        runBlocking {
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"error":"unauthorized"}""", 401) }
            assertFailsWith<AuthRequiredException> {
                RemoteDb(url).putWithAttachments("doc", JsonObject(emptyMap()), listOf(attachment())) { "abcd".toByteArray().inputStream() }
            }
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"error":"internal"}""", 503) }
            assertEquals(
                503,
                assertFailsWith<CouchException> {
                    RemoteDb(
                        url,
                    ).putWithAttachments("doc", JsonObject(emptyMap()), listOf(attachment())) { "abcd".toByteArray().inputStream() }
                }.status,
            )
        }

    @Test
    fun `a successful multipart write sends the attachment bytes and no failure`() =
        runBlocking {
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"ok":true,"id":"doc","rev":"2-b"}""", 201) }
            val failure =
                RemoteDb(
                    url,
                ).putWithAttachments("doc", JsonObject(emptyMap()), listOf(attachment())) { "abcd".toByteArray().inputStream() }
            assertNull(failure)
            assertEquals(true, String(couch.recorded("PUT", "/db/doc").single().body.readByteArray()).endsWith("--\r\n"))
        }

    @Test
    fun `bulk get returns stubs and reports missing docs`() =
        runBlocking {
            couch.on("POST", "/db/_bulk_get") {
                FakeCouch.json(
                    """{"results":[{"id":"a","docs":[{"ok":{"_id":"a","_rev":"1-a",""" +
                        """"_attachments":{"f":{"stub":true,"digest":"md5-x"}}}}]},""" +
                        """{"id":"zz","docs":[{"error":{"id":"zz","error":"not_found","reason":"missing"}}]}]}""",
                )
            }
            val results = RemoteDb(url).bulkGet(listOf(RevRequest("a", "1-a"), RevRequest("zz")))
            assertEquals("1-a", results[0].doc!!["_rev"]!!.jsonPrimitive.content)
            assertEquals("not_found", results[1].error)
            assertEquals(null, results[1].doc)
        }

    @Test
    fun `basic auth is sent with every request`() =
        runBlocking {
            couch.on("GET", "/db") { FakeCouch.json("{}") }
            RemoteDb(url, Auth.Basic("admin", "secret")).ensureExists(create = false)
            assertEquals(
                okhttp3.Credentials.basic("admin", "secret"),
                couch.recorded("GET", "/db").single().getHeader("Authorization"),
            )
        }
}
