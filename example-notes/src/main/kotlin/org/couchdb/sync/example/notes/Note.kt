package org.couchdb.sync.example.notes

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import org.couchdb.sync.Doc

/** A note as the screens see it. */
data class Note(
    val id: String,
    val rev: String,
    val title: String,
    val body: String,
    val pinned: Boolean,
    val updatedAt: Long,
) {
    companion object {
        fun from(doc: Doc) =
            Note(
                id = doc.id,
                rev = doc.rev,
                title = doc.body.text("title"),
                body = doc.body.text("body"),
                pinned = (doc.body["pinned"] as? JsonPrimitive)?.booleanOrNull ?: false,
                updatedAt = (doc.body["updatedAt"] as? JsonPrimitive)?.longOrNull ?: 0L,
            )
    }
}

/**
 * The JSON stored for a note. Plain fields, so any CouchDB client can read
 * it. `type` lets the notes query and a sync filter pick out notes.
 */
fun noteBody(
    title: String,
    body: String,
    pinned: Boolean,
    updatedAt: Long = System.currentTimeMillis(),
): JsonObject =
    buildJsonObject {
        put("type", "note")
        put("title", title)
        put("body", body)
        put("pinned", pinned)
        put("updatedAt", updatedAt)
    }

private fun JsonObject.text(key: String): String = (this[key] as? JsonPrimitive)?.contentOrNull.orEmpty()
