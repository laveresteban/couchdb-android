package org.couchdb.sync

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CouchLiteTest {
    private fun db() = CouchLite(SqlLocalStore(JdbcSqlDriver()), MemoryBlobStore())

    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    private fun value(d: Doc?) = d?.body?.get("v")?.jsonPrimitive?.content

    /** Makes "n" conflicted: base 1-a, then two branches from it, as a remote would write them. */
    private fun SqlLocalStore.conflicted(id: String) {
        insertRevision(id, listOf("1-a"), body("A"), false)
        insertRevision(id, listOf("2-b", "1-a"), body("B"), false)
        insertRevision(id, listOf("2-c", "1-a"), body("C"), false)
    }

    @Test
    fun `pulling a descendant of a local revision is not a conflict`() =
        runBlocking<Unit> {
            val db = db()
            val rev1 = db.put("n", body("1"))
            val rev2 = db.put("n", body("2"), rev1)
            // The server has 3 and 4, with 3 unknown here: the local 2 must stop being a leaf.
            db.store.insertRevision("n", listOf("4-d", "3-c", rev2, rev1), body("4"), false)
            assertEquals(emptyList(), db.conflicts("n"))
            assertEquals("4-d", db.get("n")!!.rev)
        }

    @Test
    fun `pick makes the chosen losing revision the winner`() =
        runBlocking {
            val db = db()
            db.store.conflicted("n")
            val loser = db.conflicts("n").first()
            db.resolve("n") { _, _ -> Resolution.Pick(loser.rev) }
            assertEquals(loser.body, db.get("n")!!.body)
            assertEquals(emptyList(), db.conflicts("n"))
        }

    @Test
    fun `pick of a revision that isn't a conflict fails and changes nothing`() =
        runBlocking {
            val db = db()
            db.store.conflicted("n")
            assertFailsWith<IllegalArgumentException> { db.resolve("n") { _, _ -> Resolution.Pick("9-zzz") } }
            assertEquals(1, db.conflicts("n").size)
        }

    @Test
    fun `merge writes a new body over the winner and drops the other branches`() =
        runBlocking {
            val db = db()
            db.store.conflicted("n")
            db.resolve("n") { _, _ -> Resolution.Merge(body("merged")) }
            assertEquals("merged", value(db.get("n")))
            assertEquals(emptyList(), db.conflicts("n"))
            assertEquals(emptyList(), db.conflictIds())
        }

    @Test
    fun `keeping the winner deletes the other branches`() =
        runBlocking {
            val db = db()
            db.store.conflicted("n")
            val winner = db.get("n")!!
            assertEquals(winner.rev, db.resolve("n") { _, _ -> Resolution.KeepWinner })
            assertEquals(winner.rev, db.get("n")!!.rev)
            assertEquals(emptyList(), db.conflicts("n"))
        }

    @Test
    fun `resolving a doc without conflicts returns its winner`() =
        runBlocking {
            val db = db()
            val rev = db.put("plain", body("1"))
            assertEquals(rev, db.resolve("plain") { _, _ -> error("not called") })
            assertNull(db.resolve("missing") { _, _ -> error("not called") })
        }

    @Test
    fun `conflict observers see conflicts appear`() =
        runBlocking {
            val db = db()
            val ids = async { withTimeout(10_000) { db.observeConflictIds().first { it.isNotEmpty() } } }
            val docs = async { withTimeout(10_000) { db.observeConflicts("n").first { it.isNotEmpty() } } }
            delay(200)
            db.store.conflicted("n")
            assertEquals(listOf("n"), ids.await())
            assertEquals(1, docs.await().size)
        }

    @Test
    fun `document observers see changes made while they start`() =
        runBlocking {
            val db = db()
            val seen = async { withTimeout(10_000) { db.observe("doc").first { value(it) == "2" } } }
            db.put("doc", body("1"))
            db.put("doc", body("2"), db.get("doc")!!.rev)
            assertEquals("2", value(seen.await()))
        }

    @Test
    fun `local writes are announced by id`() =
        runBlocking {
            val db = db()
            val written = async { withTimeout(10_000) { db.localWrites.first() } }
            delay(100)
            db.put("announced", body("1"))
            assertEquals("announced", written.await())
        }

    @Test
    fun `attachments add, read and remove`() =
        runBlocking {
            val db = db()
            val rev = db.put("doc", body("1"))
            val withFile = db.putAttachment("doc", rev, "a.txt", "text/plain", "hi".toByteArray())
            assertContentEquals("hi".toByteArray(), db.getAttachment("doc", "a.txt"))
            assertEquals("text/plain", db.attachments(db.get("doc")!!).single().contentType)
            val withoutFile = db.deleteAttachment("doc", withFile, "a.txt")
            assertNull(db.getAttachment("doc", "a.txt"))
            assertTrue(withoutFile.startsWith("3-"))
        }

    @Test
    fun `closed databases refuse calls`() =
        runBlocking {
            val db = db()
            db.put("x", body("1"))
            db.close()
            assertFailsWith<Exception> { db.get("x") }
        }
}
