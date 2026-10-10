package org.couchdb.sync

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CompoundIndexTest {
    private fun json(s: String) = Json.parseToJsonElement(s).jsonObject

    /** Mango order, with a missing field below null. */
    private fun cmp(
        a: JsonElement?,
        b: JsonElement?,
    ): Int =
        when {
            a == null && b == null -> 0
            a == null -> -1
            b == null -> 1
            else -> Mango.compare(a, b)
        }

    private fun single(v: JsonElement?) = if (v == null) JsonObject(emptyMap()) else JsonObject(mapOf("f" to v))

    private val scalars: List<JsonElement?> =
        listOf(
            null, JsonNull, JsonPrimitive(false), JsonPrimitive(true), JsonPrimitive(-1e9), JsonPrimitive(-0.5),
            JsonPrimitive(0), JsonPrimitive(0.25), JsonPrimitive(1e12), JsonPrimitive(""), JsonPrimitive("a"),
            JsonPrimitive("a\u0000"), JsonPrimitive("ab"), JsonPrimitive("b"), JsonPrimitive("é"), JsonPrimitive("中文"),
            JsonArray(listOf(JsonPrimitive(1))),
        )

    @Test
    fun `sort keys order single values like Mango compare`() {
        for (a in scalars) for (b in scalars) {
            val expected = Integer.signum(cmp(a, b))
            val actual = Integer.signum(sortKey(single(a), listOf("f")).compareTo(sortKey(single(b), listOf("f"))))
            assertEquals(expected, actual, "$a vs $b")
        }
    }

    @Test
    fun `sort keys order field tuples like comparing field by field`() {
        val fields = listOf("f", "g")
        val pairs = listOf(null, JsonNull, JsonPrimitive(false), JsonPrimitive(2), JsonPrimitive("x"), JsonPrimitive("xy"))
        for (a1 in pairs) for (a2 in pairs) for (b1 in pairs) for (b2 in pairs) {
            val a = JsonObject(listOfNotNull(a1?.let { "f" to it }, a2?.let { "g" to it }).toMap())
            val b = JsonObject(listOfNotNull(b1?.let { "f" to it }, b2?.let { "g" to it }).toMap())
            val expected = cmp(a1, b1).takeIf { it != 0 } ?: cmp(a2, b2)
            val actual = sortKey(a, fields).compareTo(sortKey(b, fields))
            assertEquals(Integer.signum(expected), Integer.signum(actual), "$a vs $b")
        }
    }

    @Test
    fun `compound index sort matches an in-memory sort in both directions`() {
        val docs =
            mapOf(
                "a" to """{"team":"x","score":2}""",
                "b" to """{"team":"x","score":1}""",
                "c" to """{"team":"w","score":3}""",
                "d" to """{"team":"x"}""",
                "e" to """{"team":"x","score":"high"}""",
            )
        val plain = SqlLocalStore(JdbcSqlDriver()).also { s -> docs.forEach { (id, b) -> s.put(id, json(b), null) } }
        val indexed =
            SqlLocalStore(JdbcSqlDriver()).also { s ->
                docs.forEach { (id, b) -> s.put(id, json(b), null) }
                s.createCompoundIndex(listOf("team", "score"))
            }
        for (asc in listOf(true, false)) {
            val sort = listOf("team" to asc, "score" to asc)
            val query = Query(JsonObject(emptyMap()), sort = sort)
            assertEquals(plain.find(query).map { it.id }, indexed.find(query).map { it.id }, "asc=$asc")
        }
        val ascending = indexed.find(Query(JsonObject(emptyMap()), sort = listOf("team" to true, "score" to true)))
        assertEquals(listOf("c", "d", "b", "a", "e"), ascending.map { it.id })
    }

    @Test
    fun `index follows updates and deletes`() {
        val store = SqlLocalStore(JdbcSqlDriver())
        store.createCompoundIndex(listOf("score"))
        val r1 = store.put("a", json("""{"score":1}"""), null)
        store.put("b", json("""{"score":2}"""), null)
        store.put("a", json("""{"score":3}"""), r1)
        val asc = Query(JsonObject(emptyMap()), sort = listOf("score" to true))
        assertEquals(listOf("b", "a"), store.find(asc).map { it.id })
        store.delete("b", store.get("b")!!.rev)
        assertEquals(listOf("a"), store.find(asc).map { it.id })
    }

    @Test
    fun `an index created after writes covers existing docs`() {
        val store = SqlLocalStore(JdbcSqlDriver())
        store.put("b", json("""{"score":2}"""), null)
        store.put("a", json("""{"score":1}"""), null)
        store.createCompoundIndex(listOf("score"))
        assertEquals(listOf("a", "b"), store.find(Query(JsonObject(emptyMap()), sort = listOf("score" to true))).map { it.id })
    }

    @Test
    fun `a sort that doesn't match an index still sorts in memory`() {
        val store = SqlLocalStore(JdbcSqlDriver())
        store.createCompoundIndex(listOf("team", "score"))
        store.put("a", json("""{"team":"x","score":1}"""), null)
        store.put("b", json("""{"team":"w","score":5}"""), null)
        val mixed = Query(JsonObject(emptyMap()), sort = listOf("team" to true, "score" to false))
        assertEquals(listOf("b", "a"), store.find(mixed).map { it.id })
        val notPrefix = Query(JsonObject(emptyMap()), sort = listOf("score" to true))
        assertEquals(listOf("a", "b"), store.find(notPrefix).map { it.id })
    }

    @Test
    fun `index fields must be named`() {
        val store = SqlLocalStore(JdbcSqlDriver())
        assertFailsWith<IllegalArgumentException> { store.createCompoundIndex(emptyList()) }
    }
}
