package org.couchdb.sync

import java.io.File
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/** Attachment content, keyed by CouchDB digest (`md5-<base64>`). */
interface BlobStore {
    fun put(digest: String, data: ByteArray)
    fun get(digest: String): ByteArray?
    fun has(digest: String): Boolean
    fun digests(): Set<String>
    fun delete(digest: String)
}

/** CouchDB's attachment digest for unencoded content. */
fun attachmentDigest(data: ByteArray): String =
    "md5-" + Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(data))

/**
 * Stores each attachment as a file in [dir]. With a 32-byte [key], files are
 * encrypted with AES-256-GCM (random 12-byte IV prepended), so attachments
 * are protected at rest the same way an encrypted database is.
 *
 * Files are written to a temp name and renamed, so a crash never leaves a
 * half-written blob under a real digest.
 */
class FileBlobStore(private val dir: File, private val key: ByteArray? = null) : BlobStore {
    init {
        require(key == null || key.size == 32) { "key must be 32 bytes" }
        dir.mkdirs()
    }

    private val random = SecureRandom()

    override fun put(digest: String, data: ByteArray) {
        val target = file(digest)
        if (target.exists()) return
        File(dir, target.name + ".digest").writeText(digest)
        val tmp = File(dir, target.name + ".tmp")
        tmp.writeBytes(encrypt(data))
        if (!tmp.renameTo(target)) {
            tmp.delete()
            check(target.exists()) { "could not store blob $digest" }
        }
    }

    override fun get(digest: String): ByteArray? = file(digest).takeIf { it.exists() }?.readBytes()?.let(::decrypt)

    override fun has(digest: String) = file(digest).exists()

    override fun digests(): Set<String> =
        dir.listFiles { f -> f.name.endsWith(".digest") }.orEmpty().map { it.readText() }.toSet()

    override fun delete(digest: String) {
        file(digest).delete()
        File(dir, file(digest).name + ".digest").delete()
    }

    // Digests contain '/' and '+', so name files by a hash of the digest.
    private fun file(digest: String) = File(
        dir,
        MessageDigest.getInstance("SHA-256").digest(digest.toByteArray()).joinToString("") { "%02x".format(it) },
    )

    private fun encrypt(data: ByteArray): ByteArray {
        if (key == null) return data
        val iv = ByteArray(12).also(random::nextBytes)
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv))
        return iv + c.doFinal(data)
    }

    private fun decrypt(data: ByteArray): ByteArray {
        if (key == null) return data
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, data, 0, 12))
        return c.doFinal(data, 12, data.size - 12)
    }
}

/** For tests and throwaway databases. */
class MemoryBlobStore : BlobStore {
    private val blobs = java.util.concurrent.ConcurrentHashMap<String, ByteArray>()
    override fun put(digest: String, data: ByteArray) { blobs.putIfAbsent(digest, data) }
    override fun get(digest: String) = blobs[digest]
    override fun has(digest: String) = blobs.containsKey(digest)
    override fun digests() = blobs.keys.toSet()
    override fun delete(digest: String) { blobs.remove(digest) }
}
