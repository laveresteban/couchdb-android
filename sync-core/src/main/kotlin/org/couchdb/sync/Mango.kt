package org.couchdb.sync

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** A local query in Mango form, the same shape as CouchDB `_find`. */
data class Query(
    val selector: JsonObject,
    /** Field paths with `true` for ascending. */
    val sort: List<Pair<String, Boolean>> = emptyList(),
    val limit: Int? = null,
    val skip: Int = 0,
)

/**
 * Evaluates Mango selectors against a document body. Supports `$and`,
 * `$or`, `$nor`, `$not`, implicit equality, nested objects and dotted paths,
 * and the operators `$eq $ne $gt $gte $lt $lte $in $nin $exists $type
 * $size $all $elemMatch $regex $mod`.
 *
 * As in CouchDB, a missing field only matches `{"$exists": false}`.
 */
object Mango {
    fun matches(doc: JsonObject, selector: JsonObject): Boolean =
        selector.all { (key, cond) -> matchTop(doc, key, cond) }

    private fun matchTop(doc: JsonObject, key: String, cond: JsonElement): Boolean = when (key) {
        "\$and" -> cond.jsonArray.all { matches(doc, it.jsonObject) }
        "\$or" -> cond.jsonArray.any { matches(doc, it.jsonObject) }
        "\$nor" -> cond.jsonArray.none { matches(doc, it.jsonObject) }
        "\$not" -> !matches(doc, cond.jsonObject)
        else -> matchField(field(doc, key), cond, key, doc)
    }

    private fun matchField(value: JsonElement?, cond: JsonElement, path: String, doc: JsonObject): Boolean {
        if (cond is JsonObject && cond.keys.any { it.startsWith("$") }) {
            return cond.all { (op, arg) -> operator(value, op, arg) }
        }
        if (cond is JsonObject) {
            // {"a": {"b": 1}} is the same as {"a.b": 1}
            return cond.all { (k, c) -> matchTop(doc, "$path.$k", c) }
        }
        return value != null && compare(value, cond) == 0
    }

    private fun operator(value: JsonElement?, op: String, arg: JsonElement): Boolean {
        if (op == "\$exists") return (value != null) == arg.jsonPrimitive.booleanOrNull
        if (value == null) return false
        return when (op) {
            "\$eq" -> compare(value, arg) == 0
            "\$ne" -> compare(value, arg) != 0
            "\$gt" -> compare(value, arg) > 0
            "\$gte" -> compare(value, arg) >= 0
            "\$lt" -> compare(value, arg) < 0
            "\$lte" -> compare(value, arg) <= 0
            "\$in" -> arg.jsonArray.any { inArray(value, it) }
            "\$nin" -> arg.jsonArray.none { inArray(value, it) }
            "\$type" -> typeName(value) == arg.jsonPrimitive.content
            "\$size" -> value is JsonArray && value.size == arg.jsonPrimitive.content.toInt()
            "\$all" -> value is JsonArray && arg.jsonArray.all { a -> value.any { compare(it, a) == 0 } }
            "\$elemMatch" -> value is JsonArray && value.any { el ->
                val sel = arg.jsonObject
                if (el is JsonObject && sel.keys.none { it.startsWith("$") }) matches(el, sel)
                else matchField(el, sel, "", JsonObject(emptyMap()))
            }
            "\$regex" -> value is JsonPrimitive && value.isString &&
                Regex(arg.jsonPrimitive.content).containsMatchIn(value.content)
            "\$mod" -> {
                val (d, r) = arg.jsonArray.map { it.jsonPrimitive.content.toLong() }
                (value as? JsonPrimitive)?.content?.toLongOrNull()?.let { it % d == r } ?: false
            }
            "\$not" -> !matchField(value, arg, "", JsonObject(emptyMap()))
            else -> throw IllegalArgumentException("unsupported operator $op")
        }
    }

    /** `$in` matches an equal value, or any element when the field is an array. */
    private fun inArray(value: JsonElement, candidate: JsonElement) =
        compare(value, candidate) == 0 || (value is JsonArray && value.any { compare(it, candidate) == 0 })

    fun field(doc: JsonObject, path: String): JsonElement? {
        var cur: JsonElement = doc
        for (part in path.split('.')) {
            cur = when (cur) {
                is JsonObject -> cur[part] ?: return null
                is JsonArray -> part.toIntOrNull()?.let { cur.jsonArray.getOrNull(it) } ?: return null
                else -> return null
            }
        }
        return cur
    }

    private fun typeName(v: JsonElement) = when {
        v is JsonNull -> "null"
        v is JsonObject -> "object"
        v is JsonArray -> "array"
        v is JsonPrimitive && v.isString -> "string"
        v is JsonPrimitive && v.booleanOrNull != null -> "boolean"
        else -> "number"
    }

    /** Collation rank: null < false < true < numbers < strings < arrays < objects. */
    internal fun rank(v: JsonElement): Int = when {
        v is JsonNull -> 0
        v is JsonPrimitive && !v.isString && v.booleanOrNull == false -> 1
        v is JsonPrimitive && !v.isString && v.booleanOrNull == true -> 2
        v is JsonPrimitive && !v.isString -> 3
        v is JsonPrimitive -> 4
        v is JsonArray -> 5
        else -> 6
    }

    /** CouchDB view collation, with plain code-point order for strings. */
    fun compare(a: JsonElement, b: JsonElement): Int {
        val ra = rank(a)
        val rb = rank(b)
        if (ra != rb) return ra.compareTo(rb)
        return when (ra) {
            3 -> a.jsonPrimitive.doubleOrNull!!.compareTo(b.jsonPrimitive.doubleOrNull!!)
            4 -> a.jsonPrimitive.content.compareTo(b.jsonPrimitive.content)
            5 -> {
                val x = a.jsonArray
                val y = b.jsonArray
                for (i in 0 until minOf(x.size, y.size)) compare(x[i], y[i]).let { if (it != 0) return it }
                x.size.compareTo(y.size)
            }
            6 -> {
                val x = a.jsonObject.entries.toList()
                val y = b.jsonObject.entries.toList()
                for (i in 0 until minOf(x.size, y.size)) {
                    x[i].key.compareTo(y[i].key).let { if (it != 0) return it }
                    compare(x[i].value, y[i].value).let { if (it != 0) return it }
                }
                x.size.compareTo(y.size)
            }
            else -> 0
        }
    }
}
