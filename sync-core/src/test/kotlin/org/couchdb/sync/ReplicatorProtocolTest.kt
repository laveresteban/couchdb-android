package org.couchdb.sync

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Replication against [FakeCouch]: checks the wire protocol without a real server. */
class ReplicatorProtocolTest {
    private val server = MockWebServer()
    private val couch = FakeCouch()
    private val url by lazy { server.url("/db").toString() }

    @BeforeEach
    fun start() {
        server.dispatcher = couch
        server.start()
        couch.on("GET", "/db") { FakeCouch.json("{}") }
        couch.withEmptyCheckpoints()
        couch.on("GET", "/db/_changes") { FakeCouch.json(changes()) }
        couch.on("POST", "/db/_revs_diff") { FakeCouch.json("{}") }
    }

    @AfterEach
    fun stop() {
        server.shutdown()
    }

    private fun device(blobs: BlobStore = MemoryBlobStore()) = CouchLite(SqlLocalStore(JdbcSqlDriver()), blobs)

    private fun remote() = RemoteDb(url)

    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    private fun changes(
        vararg rows: String,
        pending: Int? = null,
    ) = """{"results":[${rows.joinToString(",")}],"last_seq":"9"${pending?.let { ",\"pending\":$it" }.orEmpty()}}"""

    private fun doc(
        id: String,
        rev: String,
        atts: String = "{}",
        v: String = "x",
    ) = """{"ok":{"_id":"$id","_rev":"$rev",""" +
        """"_revisions":{"start":${rev.substringBefore('-')},"ids":["${rev.substringAfter('-')}"]},"v":"$v","_attachments":$atts}}"""

    private fun bulkGet(
        id: String,
        vararg docs: String,
    ) = """{"results":[{"id":"$id","docs":[${docs.joinToString(",")}]}]}"""

    private fun stub(
        name: String,
        digest: String,
        length: Int,
    ) = """{"$name":{"stub":true,"content_type":"application/octet-stream","digest":"$digest","length":$length,"revpos":1}}"""

    @Test
    fun `pulled attachments are streamed into the blob store by digest`() =
        runBlocking {
            val bytes = ByteArray(5000) { it.toByte() }
            couch.on("GET", "/db/_changes") { FakeCouch.json(changes("""{"seq":"1","id":"doc","changes":[{"rev":"1-a"}]}""")) }
            couch.on("POST", "/db/_bulk_get") {
                FakeCouch.json(bulkGet("doc", doc("doc", "1-a", stub("photo.bin", attachmentDigest(bytes), bytes.size))))
            }
            couch.on("GET", "/db/doc/photo.bin") { MockResponse().setBody(Buffer().write(bytes)) }

            val phone = device()
            assertEquals(1, phone.replicator(remote()).sync().pulled)
            assertContentEquals(bytes, phone.getAttachment("doc", "photo.bin"))
        }

    @Test
    fun `attachment content already stored isn't downloaded again`() =
        runBlocking {
            val bytes = "already here".toByteArray()
            val blobs = MemoryBlobStore().apply { put(attachmentDigest(bytes), bytes) }
            couch.on("GET", "/db/_changes") { FakeCouch.json(changes("""{"seq":"1","id":"doc","changes":[{"rev":"1-a"}]}""")) }
            couch.on(
                "POST",
                "/db/_bulk_get",
            ) { FakeCouch.json(bulkGet("doc", doc("doc", "1-a", stub("a.txt", attachmentDigest(bytes), bytes.size)))) }

            device(blobs).replicator(remote()).sync()
            assertEquals(0, couch.recorded("GET", "/db/doc/a.txt").size)
        }

    @Test
    fun `attachment content that fails its digest check is an error and isn't stored`() =
        runBlocking {
            val expected = "the real bytes".toByteArray()
            couch.on("GET", "/db/_changes") { FakeCouch.json(changes("""{"seq":"1","id":"doc","changes":[{"rev":"1-a"}]}""")) }
            couch.on("POST", "/db/_bulk_get") {
                FakeCouch.json(bulkGet("doc", doc("doc", "1-a", stub("a.txt", attachmentDigest(expected), expected.size))))
            }
            couch.on("GET", "/db/doc/a.txt") { MockResponse().setBody("tampered!!!!!!!".toByteArray().let { Buffer().write(it) }) }

            val phone = device()
            val result = phone.replicator(remote()).sync()
            assertEquals(0, result.pulled)
            assertEquals(1, result.pullErrors)
            assertNull(phone.get("doc"))
        }

    @Test
    fun `large attachments are sent as multipart parts with a matching length`() =
        runBlocking {
            val bytes = ByteArray(3000) { (it % 251).toByte() }
            val phone = device()
            val rev1 = phone.put("doc", body("1"))
            val rev2 = phone.putAttachment("doc", rev1, "big.bin", "application/octet-stream", bytes)
            couch.on("POST", "/db/_revs_diff") { FakeCouch.json("""{"doc":{"missing":["$rev2"]}}""") }
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"ok":true,"id":"doc","rev":"$rev2"}""", 201) }

            val result = Replicator(phone.store, remote(), phone.blobs, inlineAttachmentBytes = 1000).sync()
            assertEquals(1, result.pushed)

            val put = couch.recorded("PUT", "/db/doc").single()
            assertEquals("false", put.requestUrl!!.queryParameter("new_edits"))
            assertTrue(put.getHeader("Content-Type")!!.startsWith("multipart/related; boundary="))
            val sent = put.body.readByteArray()
            assertEquals(sent.size.toString(), put.getHeader("Content-Length"))
            val text = String(sent, Charsets.ISO_8859_1)
            assertTrue(text.contains("\"follows\":true"), text.take(300))
            assertTrue(text.contains("filename=\"big.bin\""))
            assertTrue(text.contains(String(bytes, Charsets.ISO_8859_1)))
        }

    @Test
    fun `small attachments stay inline in the bulk write`() =
        runBlocking {
            val phone = device()
            val rev1 = phone.put("doc", body("1"))
            val rev2 = phone.putAttachment("doc", rev1, "small.txt", "text/plain", "tiny".toByteArray())
            couch.on("POST", "/db/_revs_diff") { FakeCouch.json("""{"doc":{"missing":["$rev2"]}}""") }
            couch.on("POST", "/db/_bulk_docs") { FakeCouch.json("[]", 201) }

            phone.replicator(remote()).sync()
            val bulk = String(couch.recorded("POST", "/db/_bulk_docs").single().body.readByteArray())
            assertTrue(bulk.contains("\"data\":\"dGlueQ==\""), bulk)
            assertEquals(0, couch.recorded("PUT", "/db/doc").size)
        }

    @Test
    fun `a multipart write the server refuses is reported and doesn't stop the sync`() =
        runBlocking {
            val phone = device()
            val rev1 = phone.put("doc", body("1"))
            val rev2 = phone.putAttachment("doc", rev1, "big.bin", "application/octet-stream", ByteArray(2000))
            couch.on("POST", "/db/_revs_diff") { FakeCouch.json("""{"doc":{"missing":["$rev2"]}}""") }
            couch.on("PUT", "/db/doc") { FakeCouch.json("""{"error":"forbidden","reason":"no big files"}""", 403) }

            val result = Replicator(phone.store, remote(), phone.blobs, inlineAttachmentBytes = 1000).sync()
            assertEquals(0, result.pushed)
            assertEquals(listOf("doc" to "forbidden"), result.pushFailures.map { it.id to it.error })
        }

    @Test
    fun `conflicts from a pull are settled by the resolver before the push`() =
        runBlocking {
            couch.on("GET", "/db/_changes") {
                FakeCouch.json(changes("""{"seq":"1","id":"n","changes":[{"rev":"2-b"},{"rev":"2-c"}]}"""))
            }
            couch.on("POST", "/db/_bulk_get") {
                FakeCouch.json(bulkGet("n", doc("n", "2-b", v = "from B"), doc("n", "2-c", v = "from C")))
            }
            val phone = device()
            phone.replicator(remote(), resolver = ConflictResolver { _, others -> Resolution.Pick(others.first().rev) }).sync()

            assertEquals(0, phone.conflicts("n").size)
            assertEquals(0, phone.conflictIds().size)
            assertEquals(1, couch.recorded("POST", "/db/_revs_diff").size)
        }

    @Test
    fun `pull reports how many changes the server still has`() =
        runBlocking {
            couch.on("GET", "/db/_changes") {
                FakeCouch.json(changes("""{"seq":"1","id":"a","changes":[{"rev":"1-a"}]}""", pending = 5))
            }
            couch.on("POST", "/db/_bulk_get") { FakeCouch.json(bulkGet("a", doc("a", "1-a"))) }
            val states = mutableListOf<SyncState>()
            val rep = device().replicator(remote())
            val watcher = kotlinx.coroutines.CoroutineScope(Dispatchers.Unconfined).launch { rep.state.collect { states += it } }
            rep.sync()
            watcher.cancel()
            assertTrue(states.contains(SyncState.Pulling(1, 5L)), "$states")
        }

    @Test
    fun `push reports the local changes it has left after each batch`() =
        runBlocking {
            val phone = device()
            phone.put("a", body("1"))
            phone.put("b", body("1"))
            val states = mutableListOf<SyncState>()
            val rep = Replicator(phone.store, remote(), phone.blobs, batchSize = 1)
            val watcher = kotlinx.coroutines.CoroutineScope(Dispatchers.Unconfined).launch { rep.state.collect { states += it } }
            rep.sync()
            watcher.cancel()
            assertTrue(states.contains(SyncState.Pushing(0, 1L)), "$states")
        }
}
