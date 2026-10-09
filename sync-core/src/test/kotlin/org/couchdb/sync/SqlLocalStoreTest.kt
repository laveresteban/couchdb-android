package org.couchdb.sync

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SqlLocalStoreTest {
    private val store = SqlLocalStore(JdbcSqlDriver())

    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    @Test
    fun `create, update and delete`() {
        val r1 = store.put("a", body("1"), null)
        assertTrue(r1.startsWith("1-"))
        val r2 = store.put("a", body("2"), r1)
        assertTrue(r2.startsWith("2-"))
        assertEquals("2", store.get("a")!!.body["v"]!!.jsonPrimitive.content)
        store.delete("a", r2)
        assertNull(store.get("a"))
    }

    @Test
    fun `stale or missing rev is a conflict`() {
        val r1 = store.put("a", body("1"), null)
        store.put("a", body("2"), r1)
        assertFailsWith<ConflictException> { store.put("a", body("3"), r1) }
        assertFailsWith<ConflictException> { store.put("a", body("3"), null) }
    }

    @Test
    fun `recreating a deleted doc extends the tombstone`() {
        val r1 = store.put("a", body("1"), null)
        store.delete("a", r1)
        val r3 = store.put("a", body("again"), null)
        assertTrue(r3.startsWith("3-"))
    }

    @Test
    fun `metadata fields are not stored in the body`() {
        store.put("a", JsonObject(mapOf("_id" to JsonPrimitive("x"), "_rev" to JsonPrimitive("9-z"), "v" to JsonPrimitive(1))), null)
        assertEquals(setOf("v"), store.get("a")!!.body.keys)
    }

    @Test
    fun `winner matches CouchDB rules`() {
        store.insertRevision("x", listOf("1-aaa"), body("a"), false)
        store.insertRevision("x", listOf("1-bbb"), body("b"), false)
        assertEquals("1-bbb", store.get("x")!!.rev) // same generation: higher hash wins
        assertEquals(listOf("1-aaa"), store.conflicts("x").map { it.rev })

        store.insertRevision("x", listOf("2-ccc", "1-aaa"), body("c"), false)
        assertEquals("2-ccc", store.get("x")!!.rev) // longer branch wins

        store.insertRevision("x", listOf("3-ddd", "2-ccc", "1-aaa"), JsonObject(emptyMap()), true)
        assertEquals("1-bbb", store.get("x")!!.rev) // non-deleted beats deleted
    }

    @Test
    fun `inserted history fills in ancestors`() {
        assertTrue(store.insertRevision("d", listOf("3-c", "2-b", "1-a"), body("3"), false))
        assertEquals(listOf("3-c", "2-b", "1-a"), store.revisionPath("d", "3-c"))
        assertEquals(listOf("3-c"), store.leafRevs("d"))
        assertEquals(mapOf("d" to listOf("4-x")), store.missingRevs(mapOf("d" to listOf("2-b", "4-x"))))
        assertEquals(false, store.insertRevision("d", listOf("3-c", "2-b", "1-a"), body("3"), false))
    }

    @Test
    fun `changes are ordered by latest local sequence`() {
        val ra = store.put("a", body("1"), null)
        store.put("b", body("1"), null)
        store.put("a", body("2"), ra)
        assertEquals(listOf("b", "a"), store.changesSince(0, 10).map { it.id })
        assertEquals(listOf("a"), store.changesSince(store.changesSince(0, 10).first().seq, 10).map { it.id })
    }
}
