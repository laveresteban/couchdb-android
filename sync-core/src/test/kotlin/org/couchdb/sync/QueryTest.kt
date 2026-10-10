package org.couchdb.sync

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class QueryTest {
    private fun json(s: String) = Json.parseToJsonElement(s).jsonObject

    private fun sel(s: String) = json(s)

    private val people =
        mapOf(
            "ann" to """{"name":"ann","age":31,"tags":["admin","dev"],"address":{"city":"Oslo"}}""",
            "bob" to """{"name":"bob","age":25,"tags":["dev"],"address":{"city":"Lima"}}""",
            "cat" to """{"name":"cat","age":40,"active":false}""",
            "dan" to """{"name":"dan","age":"unknown"}""",
        )

    private fun store() = SqlLocalStore(JdbcSqlDriver()).also { s -> people.forEach { (id, b) -> s.put(id, json(b), null) } }

    private fun ids(
        store: SqlLocalStore,
        selector: String,
        sort: List<Pair<String, Boolean>> = emptyList(),
    ) = store.find(Query(sel(selector), sort)).map { it.id }

    @Test
    fun `operators match like Mango`() {
        val doc = json(people["ann"]!!)
        assertTrue(Mango.matches(doc, sel("""{"age":{"${'$'}gt":30,"${'$'}lte":31}}""")))
        assertTrue(Mango.matches(doc, sel("""{"address.city":"Oslo"}""")))
        assertTrue(Mango.matches(doc, sel("""{"address":{"city":"Oslo"}}""")))
        assertTrue(Mango.matches(doc, sel("""{"tags":{"${'$'}all":["dev","admin"]}}""")))
        assertTrue(Mango.matches(doc, sel("""{"tags":{"${'$'}elemMatch":{"${'$'}eq":"admin"}}}""")))
        assertTrue(Mango.matches(doc, sel("""{"tags":{"${'$'}size":2}}""")))
        assertTrue(Mango.matches(doc, sel("""{"name":{"${'$'}regex":"^a"}}""")))
        assertTrue(Mango.matches(doc, sel("""{"${'$'}or":[{"age":1},{"name":"ann"}]}""")))
        assertTrue(Mango.matches(doc, sel("""{"missing":{"${'$'}exists":false}}""")))
        assertFalse(Mango.matches(doc, sel("""{"missing":{"${'$'}ne":1}}"""))) // missing fields only match $exists:false
        assertFalse(Mango.matches(doc, sel("""{"${'$'}not":{"name":"ann"}}""")))
        assertTrue(Mango.matches(doc, sel("""{"age":{"${'$'}type":"number"}}""")))
    }

    @Test
    fun `find with and without an index gives the same results`() {
        val s = store()
        val queries =
            listOf(
                """{"age":{"${'$'}gte":30}}""",
                """{"age":25}""",
                """{"name":{"${'$'}in":["bob","dan"]}}""",
                """{"tags":{"${'$'}in":["admin"]}}""",
                """{"age":{"${'$'}lt":"z"}}""",
            )
        val before = queries.map { ids(s, it) }
        s.createIndex("age")
        s.createIndex("name")
        s.createIndex("tags")
        assertEquals(before, queries.map { ids(s, it) })
        // Ranges compare across types: strings sort after numbers.
        assertEquals(listOf("ann", "cat", "dan"), before[0])
        assertEquals(listOf("ann"), before[3])
        assertEquals(listOf("ann", "bob", "cat", "dan"), before[4])
    }

    @Test
    fun `index follows updates and deletes`() {
        val s = store()
        s.createIndex("age")
        val bob = s.get("bob")!!
        s.put("bob", json("""{"name":"bob","age":50}"""), bob.rev)
        assertEquals(listOf("bob", "cat", "dan"), ids(s, """{"age":{"${'$'}gte":40}}"""))
        s.delete("cat", s.get("cat")!!.rev)
        assertEquals(listOf("bob", "dan"), ids(s, """{"age":{"${'$'}gte":40}}"""))
    }

    @Test
    fun `sort, skip and limit`() {
        val s = store()
        assertEquals(listOf("cat", "ann", "bob"), ids(s, """{"age":{"${'$'}type":"number"}}""", listOf("age" to false)))
        assertEquals(
            listOf("ann"),
            s.find(Query(sel("""{"age":{"${'$'}type":"number"}}"""), listOf("age" to false), limit = 1, skip = 1)).map { it.id },
        )
    }

    @Test
    fun `observe re-runs the query on change`() =
        runBlocking {
            val db = CouchLite(store())
            val q = Query(sel("""{"age":{"${'$'}gte":40,"${'$'}type":"number"}}"""))
            val seen = mutableListOf<List<String>>()
            val job = launch { db.observe(q).take(2).toList().forEach { seen += it.map(Doc::id) } }
            kotlinx.coroutines.delay(300) // let the collector subscribe
            db.put("eve", json("""{"age":44}"""))
            withTimeout(5_000) { job.join() }
            assertEquals(listOf(listOf("cat"), listOf("cat", "eve")), seen)
            assertEquals(listOf("cat", "eve"), db.observe(q).first().map { it.id })
        }
}
