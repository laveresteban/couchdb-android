package org.couchdb.sync

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.nio.file.Files
import javax.crypto.AEADBadTagException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StorageTest {
    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    @Test
    fun `encrypted blobs are unreadable without the key`() {
        val dir = Files.createTempDirectory("blobs").toFile()
        val key = ByteArray(32) { it.toByte() }
        val data = "secret photo bytes".toByteArray()
        val digest = attachmentDigest(data)
        FileBlobStore(dir, key).put(digest, data)

        val raw = dir.listFiles()!!.filterNot { it.name.endsWith(".digest") }.single().readBytes()
        assertFalse(String(raw).contains("secret"))
        assertContentEquals(data, FileBlobStore(dir, key).get(digest))
        assertFailsWith<AEADBadTagException> { FileBlobStore(dir, ByteArray(32)).get(digest) }
        assertEquals(setOf(digest), FileBlobStore(dir, key).digests())
    }

    @Test
    fun `digest matches CouchDB format`() {
        // md5("hello") = 5d41402abc4b2a76b9719d911017c592
        assertEquals("md5-XUFAKrxLKna5cZ2REBfFkg==", attachmentDigest("hello".toByteArray()))
    }

    @Test
    fun `attachments are stored by digest and survive edits that keep their stubs`() = runBlocking {
        val db = CouchLite(SqlLocalStore(JdbcSqlDriver()))
        val r1 = db.put("a", body("1"))
        val r2 = db.putAttachment("a", r1, "note.txt", "text/plain", "hi".toByteArray())
        assertContentEquals("hi".toByteArray(), db.getAttachment("a", "note.txt"))
        val doc = db.get("a")!!
        val r3 = db.put("a", JsonObject(body("2") + ("_attachments" to doc.body["_attachments"]!!)), r2)
        assertContentEquals("hi".toByteArray(), db.getAttachment("a", "note.txt"))
        db.deleteAttachment("a", r3, "note.txt")
        assertNull(db.getAttachment("a", "note.txt"))
    }

    @Test
    fun `compaction drops old bodies, history and unused blobs`() = runBlocking {
        val db = CouchLite(SqlLocalStore(JdbcSqlDriver()))
        var rev = db.put("a", body("0"))
        rev = db.putAttachment("a", rev, "f", "text/plain", "old".toByteArray())
        rev = db.deleteAttachment("a", rev, "f")
        repeat(10) { rev = db.put("a", body("${it + 1}"), rev) }
        val oldest = db.store.revisionPath("a", rev).last()
        assertTrue(db.store.getRev("a", oldest) != null)

        db.compact(revsLimit = 5)
        assertNull(db.store.getRev("a", oldest))
        assertEquals(6, db.store.revisionPath("a", rev).size) // 5 kept + the pruned parent id
        assertEquals("10", db.get("a")!!.body["v"].toString().trim('"'))
        assertTrue(db.blobs.digests().isEmpty())
    }
}
