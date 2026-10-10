package org.couchdb.sync

import java.io.ByteArrayInputStream
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class BlobStoreTest {
    private val data = "streamed attachment".toByteArray()
    private val digest = attachmentDigest(data)
    private val key = ByteArray(32) { (it * 7).toByte() }

    private fun dir() = Files.createTempDirectory("blobs").toFile()

    @Test
    fun `streamed content is stored encrypted and reads back`() {
        val dir = dir()
        val store = FileBlobStore(dir, key)
        store.put(digest, ByteArrayInputStream(data))
        assertTrue(store.has(digest))
        assertContentEquals(data, store.open(digest)!!.use { it.readBytes() })
        assertContentEquals(data, store.get(digest))
        val raw = dir.listFiles { f -> !f.name.endsWith(".digest") }!!.single().readBytes()
        assertFalse(String(raw, Charsets.ISO_8859_1).contains("streamed"))
    }

    @Test
    fun `plain file store streams without a key`() {
        val store = FileBlobStore(dir())
        store.put(digest, ByteArrayInputStream(data))
        assertContentEquals(data, store.open(digest)!!.use { it.readBytes() })
        assertEquals(setOf(digest), store.digests())
    }

    @Test
    fun `streamed content that doesn't match its digest is rejected and leaves nothing behind`() {
        val dir = dir()
        val store = FileBlobStore(dir, key)
        assertFailsWith<AttachmentDigestException> { store.put(digest, ByteArrayInputStream("tampered".toByteArray())) }
        assertFalse(store.has(digest))
        assertTrue(store.digests().isEmpty())
        assertEquals(0, dir.listFiles()!!.size)
    }

    @Test
    fun `memory store rejects streamed content that doesn't match its digest`() {
        val store = MemoryBlobStore()
        assertFailsWith<AttachmentDigestException> { store.put(digest, ByteArrayInputStream("tampered".toByteArray())) }
        assertFalse(store.has(digest))
        store.put(digest, ByteArrayInputStream(data))
        assertContentEquals(data, store.get(digest))
    }

    @Test
    fun `missing content opens as null and delete removes it`() {
        val store = FileBlobStore(dir(), key)
        assertNull(store.open(digest))
        assertNull(store.get(digest))
        store.put(digest, data)
        store.delete(digest)
        assertFalse(store.has(digest))
        assertTrue(store.digests().isEmpty())
    }

    @Test
    fun `keys must be 32 bytes`() {
        assertFailsWith<IllegalArgumentException> { FileBlobStore(dir(), ByteArray(16)) }
    }
}
