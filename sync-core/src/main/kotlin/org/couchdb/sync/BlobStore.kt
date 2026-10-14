package org.couchdb.sync

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.security.DigestInputStream
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.CipherOutputStream
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/** Attachment content, keyed by CouchDB digest (`md5-<base64>`). */
interface BlobStore {
    fun put(
        digest: String,
        data: ByteArray,
    )

    /** Stores content read from [source]. Throws [AttachmentDigestException] unless it matches [digest]. */
    fun put(
        digest: String,
        source: InputStream,
    )

    fun open(digest: String): InputStream?

    fun get(digest: String): ByteArray? = open(digest)?.use { it.readBytes() }

    fun has(digest: String): Boolean

    fun digests(): Set<String>

    fun delete(digest: String)
}

/** Downloaded attachment content doesn't match the digest the server gave for it. */
class AttachmentDigestException(
    message: String,
) : RuntimeException(message)

/** CouchDB's attachment digest for unencoded content. */
fun attachmentDigest(data: ByteArray): String = "md5-" + md5Base64(data)

private fun md5Base64(data: ByteArray) = Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(data))

/** Copies [source] to [sink] and fails if the content's digest isn't [digest]. */
private fun copyVerified(
    source: InputStream,
    digest: String,
    sink: OutputStream,
) {
    val md5 = MessageDigest.getInstance("MD5")
    DigestInputStream(source, md5).use { it.copyTo(sink) }
    val actual = "md5-" + Base64.getEncoder().encodeToString(md5.digest())
    if (actual != digest) throw AttachmentDigestException("attachment $digest arrived as $actual")
}

/**
 * Stores each attachment as a file in [dir]. With a 32-byte [key], files are
 * encrypted with AES-256-GCM (random 12-byte IV prepended), so attachments
 * are protected at rest the same way an encrypted database is. Writes and
 * reads stream, so large attachments never have to fit in memory.
 *
 * Files are written to a temp name and renamed, so a crash never leaves a
 * half-written blob under a real digest.
 */
class FileBlobStore(
    private val dir: File,
    private val key: ByteArray? = null,
) : BlobStore {
    init {
        require(key == null || key.size == 32) { "key must be 32 bytes" }
        dir.mkdirs()
    }

    private val random = SecureRandom()

    override fun put(
        digest: String,
        data: ByteArray,
    ) = store(digest) { it.write(data) }

    override fun put(
        digest: String,
        source: InputStream,
    ) = store(digest) { copyVerified(source, digest, it) }

    override fun open(digest: String): InputStream? {
        val file = file(digest).takeIf { it.exists() } ?: return null
        val input = file.inputStream().buffered()
        if (key == null) return input
        val iv = input.readNBytes(IV_BYTES)
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv))
        return CipherInputStream(input, c)
    }

    override fun get(digest: String): ByteArray? = file(digest).takeIf { it.exists() }?.readBytes()?.let(::decrypt)

    override fun has(digest: String) = file(digest).exists()

    override fun digests(): Set<String> =
        dir
            .listFiles { f -> f.name.endsWith(".digest") }
            .orEmpty()
            .map { it.readText() }
            .toSet()

    override fun delete(digest: String) {
        file(digest).delete()
        File(dir, file(digest).name + ".digest").delete()
    }

    private fun store(
        digest: String,
        write: (OutputStream) -> Unit,
    ) {
        val target = file(digest)
        if (target.exists()) return
        val tmp = File(dir, target.name + ".tmp")
        try {
            encrypting(tmp.outputStream().buffered()).use(write)
            if (!tmp.renameTo(target)) check(target.exists()) { "could not store blob $digest" }
            File(dir, target.name + ".digest").writeText(digest)
        } finally {
            tmp.delete()
        }
    }

    // Digests contain '/' and '+', so name files by a hash of the digest.
    private fun file(digest: String) =
        File(
            dir,
            MessageDigest.getInstance("SHA-256").digest(digest.toByteArray()).joinToString("") { "%02x".format(it) },
        )

    private fun encrypting(out: OutputStream): OutputStream {
        if (key == null) return out
        val iv = ByteArray(IV_BYTES).also(random::nextBytes)
        out.write(iv)
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, iv))
        return CipherOutputStream(out, c)
    }

    private fun decrypt(data: ByteArray): ByteArray {
        if (key == null) return data
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, data, 0, IV_BYTES))
        return c.doFinal(data, IV_BYTES, data.size - IV_BYTES)
    }

    private companion object {
        const val IV_BYTES = 12
    }
}

/** For tests and throwaway databases. */
class MemoryBlobStore : BlobStore {
    private val blobs = ConcurrentHashMap<String, ByteArray>()

    override fun put(
        digest: String,
        data: ByteArray,
    ) {
        blobs.putIfAbsent(digest, data)
    }

    override fun put(
        digest: String,
        source: InputStream,
    ) {
        val out = ByteArrayOutputStream()
        copyVerified(source, digest, out)
        blobs.putIfAbsent(digest, out.toByteArray())
    }

    override fun open(digest: String): InputStream? = blobs[digest]?.inputStream()

    override fun has(digest: String) = blobs.containsKey(digest)

    override fun digests() = blobs.keys.toSet()

    override fun delete(digest: String) {
        blobs.remove(digest)
    }
}
