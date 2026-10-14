package org.couchdb.sync

import kotlinx.serialization.json.JsonObject
import java.security.MessageDigest

/** A document revision as the app sees it. [body] never contains `_id`, `_rev` or other metadata. */
data class Doc(
    val id: String,
    val rev: String,
    val body: JsonObject,
    val deleted: Boolean = false,
)

/** A CouchDB revision id, `N-hash`. */
data class Rev(
    val gen: Int,
    val hash: String,
) : Comparable<Rev> {
    override fun toString() = "$gen-$hash"

    /** CouchDB orders revisions by generation, then by hash string. */
    override fun compareTo(other: Rev): Int = compareValuesBy(this, other, Rev::gen, Rev::hash)

    companion object {
        fun parse(rev: String): Rev {
            val dash = rev.indexOf('-')
            require(dash > 0) { "bad revision id: $rev" }
            return Rev(rev.substring(0, dash).toInt(), rev.substring(dash + 1))
        }
    }
}

class ConflictException(
    message: String,
) : RuntimeException(message)

/** Fields CouchDB manages. They are stripped from stored bodies and rebuilt on push. */
internal val META_FIELDS = setOf("_id", "_rev", "_deleted", "_revisions", "_conflicts", "_revs_info")

internal fun JsonObject.withoutMeta() = JsonObject(filterKeys { it !in META_FIELDS })

internal fun md5Hex(s: String): String = MessageDigest.getInstance("MD5").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
