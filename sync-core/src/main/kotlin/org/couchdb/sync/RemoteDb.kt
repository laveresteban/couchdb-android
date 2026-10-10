package org.couchdb.sync

import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Credentials
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resumeWithException

open class CouchException(val status: Int, val error: String?, val reason: String?) :
    IOException("HTTP $status: $error ($reason)")

class AuthRequiredException(reason: String?) : CouchException(401, "unauthorized", reason)

sealed interface Auth {
    data object None : Auth
    data class Basic(val user: String, val password: String) : Auth

    /** Cookie auth via `POST /_session`; logs in again when the cookie expires. */
    data class Session(val user: String, val password: String) : Auth
}

/** One row of `_changes?style=all_docs`. */
data class RemoteChange(val id: String, val revs: List<String>, val deleted: Boolean)

data class ChangesPage(val results: List<RemoteChange>, val lastSeq: String, val pending: Long?)

/** A `_bulk_get` result: the doc with `_revisions`, or the server's [error] (e.g. `not_found`). */
data class FetchedRev(val id: String, val doc: JsonObject?, val error: String? = null)

/**
 * One `_bulk_get` request entry; a null [rev] means the winning revision.
 * [attsSince] lists revs whose attachments we already have.
 */
data class RevRequest(val id: String, val rev: String? = null, val attsSince: List<String> = emptyList())

data class RevsDiffEntry(val missing: List<String>, val possibleAncestors: List<String>)

/** Limits sync to part of a database. Pull filters on the server, push filters locally. */
sealed interface SyncFilter {
    data class DocIds(val ids: List<String>) : SyncFilter
    data class Selector(val selector: JsonObject) : SyncFilter
}

/**
 * The replication subset of the CouchDB API, matching these
 * couchdb-openapi operations: getDatabaseInformation, putDatabase,
 * getChanges, postRevsDiff, postBulkGet, postBulkDocs (new_edits=false),
 * getLocalDocument and putLocalDocument.
 */
class RemoteDb(
    dbUrl: String,
    private val auth: Auth = Auth.None,
    client: OkHttpClient = OkHttpClient(),
) {
    val url: HttpUrl = dbUrl.trimEnd('/').toHttpUrl()
    private val server: HttpUrl = url.newBuilder().removePathSegment(url.pathSize - 1).build()
    @Volatile private var sessionCookie: String? = null

    private val http = client.newBuilder()
        .readTimeout(90, TimeUnit.SECONDS) // longer than the longpoll timeout
        .addInterceptor(AuthInterceptor())
        .build()

    /** Returns the database's info, creating it first if [create] is set and it's missing. */
    suspend fun ensureExists(create: Boolean): JsonObject {
        val info = call("GET", url, null, allow404 = true)
        if (info != null) return info.jsonObject
        if (!create) throw CouchException(404, "not_found", "database ${url.pathSegments.last()} does not exist")
        call("PUT", url, null, allow412 = true)
        return call("GET", url, null)!!.jsonObject
    }

    /**
     * Reads the changes feed with every leaf revision. With [longpollMs] the
     * request waits up to that long for a change before returning empty.
     */
    suspend fun changes(since: String, limit: Int, longpollMs: Long? = null, filter: SyncFilter? = null): ChangesPage {
        val u = url.newBuilder().addPathSegment("_changes")
            .addQueryParameter("style", "all_docs")
            .addQueryParameter("since", since)
            .addQueryParameter("limit", limit.toString())
            .apply {
                if (longpollMs != null) {
                    addQueryParameter("feed", "longpoll")
                    addQueryParameter("timeout", longpollMs.toString())
                }
                when (filter) {
                    is SyncFilter.DocIds -> addQueryParameter("filter", "_doc_ids")
                    is SyncFilter.Selector -> addQueryParameter("filter", "_selector")
                    null -> Unit
                }
            }.build()
        val body = when (filter) {
            is SyncFilter.DocIds -> buildJsonObject { put("doc_ids", JsonArray(filter.ids.map(::JsonPrimitive))) }
            is SyncFilter.Selector -> buildJsonObject { put("selector", filter.selector) }
            null -> null
        }
        val res = call(if (body == null) "GET" else "POST", u, body)!!.jsonObject
        return ChangesPage(
            results = res["results"]!!.jsonArray.map { r ->
                val o = r.jsonObject
                RemoteChange(
                    id = o.str("id")!!,
                    revs = o["changes"]!!.jsonArray.map { it.jsonObject.str("rev")!! },
                    deleted = o["deleted"]?.jsonPrimitive?.contentOrNull == "true",
                )
            },
            lastSeq = res["last_seq"]!!.seqString(),
            pending = res["pending"]?.jsonPrimitive?.contentOrNull?.toLongOrNull(),
        )
    }

    /** Which of the given revisions the server is missing, and which ancestors of them it has. */
    suspend fun revsDiff(revs: Map<String, List<String>>): Map<String, RevsDiffEntry> {
        if (revs.isEmpty()) return emptyMap()
        val body = JsonObject(revs.mapValues { (_, v) -> JsonArray(v.map(::JsonPrimitive)) })
        val res = call("POST", url.newBuilder().addPathSegment("_revs_diff").build(), body)!!.jsonObject
        fun list(o: JsonObject, k: String) = o[k]?.jsonArray?.map { it.jsonPrimitive.content }.orEmpty()
        return res.mapValues { (_, v) -> RevsDiffEntry(list(v.jsonObject, "missing"), list(v.jsonObject, "possible_ancestors")) }
    }

    /**
     * Fetches specific revisions with their history (`_bulk_get?revs=true`).
     * With [attachments], new attachment content comes back inline as base64;
     * attachments already present in a rev from `attsSince` come back as stubs.
     */
    suspend fun bulkGet(revs: List<RevRequest>, attachments: Boolean = false): List<FetchedRev> {
        if (revs.isEmpty()) return emptyList()
        val body = buildJsonObject {
            put(
                "docs",
                buildJsonArray {
                    revs.forEach { r ->
                        add(
                            buildJsonObject {
                                put("id", r.id)
                                r.rev?.let { put("rev", it) }
                                if (r.attsSince.isNotEmpty()) put("atts_since", JsonArray(r.attsSince.map(::JsonPrimitive)))
                            },
                        )
                    }
                },
            )
        }
        val u = url.newBuilder().addPathSegment("_bulk_get").addQueryParameter("revs", "true")
            .apply { if (attachments) addQueryParameter("attachments", "true") }.build()
        val res = call("POST", u, body)!!.jsonObject
        return res["results"]!!.jsonArray.flatMap { r ->
            val id = r.jsonObject.str("id")!!
            r.jsonObject["docs"]!!.jsonArray.map {
                val o = it.jsonObject
                FetchedRev(id, o["ok"]?.jsonObject, (o["error"] as? JsonObject)?.str("error"))
            }
        }
    }

    /** Writes revisions as-is, keeping their ids and history (`new_edits: false`). */
    suspend fun bulkDocsNoNewEdits(docs: List<JsonObject>) {
        if (docs.isEmpty()) return
        val body = buildJsonObject { put("new_edits", false); put("docs", JsonArray(docs)) }
        val res = call("POST", url.newBuilder().addPathSegment("_bulk_docs").build(), body)!!.jsonArray
        // With new_edits=false CouchDB only lists failures.
        res.firstOrNull { it.jsonObject["error"] != null }?.let {
            val o = it.jsonObject
            throw CouchException(500, o.str("error"), "${o.str("id")}: ${o.str("reason")}")
        }
    }

    suspend fun getLocal(id: String): JsonObject? =
        call("GET", localUrl(id), null, allow404 = true)?.jsonObject

    suspend fun putLocal(id: String, body: JsonObject) {
        call("PUT", localUrl(id), body)
    }

    /**
     * Any other CouchDB call, relative to this database. [path] segments are
     * URL-encoded; pass an empty list for the database itself. Returns null
     * for a 404.
     */
    suspend fun request(
        method: String,
        path: List<String> = emptyList(),
        query: Map<String, String> = emptyMap(),
        body: JsonElement? = null,
    ): JsonElement? {
        val u = url.newBuilder().apply {
            path.forEach { addPathSegment(it) }
            query.forEach { (k, v) -> addQueryParameter(k, v) }
        }.build()
        return call(method, u, body, allow404 = true)
    }

    /** Deletes the remote database. Returns false if it didn't exist. */
    suspend fun destroy(): Boolean = call("DELETE", url, null, allow404 = true) != null

    private fun localUrl(id: String) = url.newBuilder().addPathSegment("_local").addPathSegment(id).build()

    private suspend fun call(
        method: String,
        u: HttpUrl,
        body: JsonElement?,
        allow404: Boolean = false,
        allow412: Boolean = false,
    ): JsonElement? {
        val req = Request.Builder().url(u)
            .header("Accept", "application/json")
            .method(method, body?.toString()?.toRequestBody(JSON) ?: if (method == "PUT" || method == "POST") ByteArray(0).toRequestBody(JSON) else null)
            .build()
        http.newCall(req).await().use { res ->
            val text = res.body?.string().orEmpty()
            if (res.isSuccessful) return Json.parseToJsonElement(text)
            if (allow404 && res.code == 404) return null
            if (allow412 && res.code == 412) return null
            val err = runCatching { Json.parseToJsonElement(text).jsonObject }.getOrNull()
            if (res.code == 401) throw AuthRequiredException(err?.str("reason"))
            throw CouchException(res.code, err?.str("error"), err?.str("reason"))
        }
    }

    private inner class AuthInterceptor : Interceptor {
        override fun intercept(chain: Interceptor.Chain): Response = when (val a = auth) {
            Auth.None -> chain.proceed(chain.request())
            is Auth.Basic -> chain.proceed(
                chain.request().newBuilder().header("Authorization", Credentials.basic(a.user, a.password)).build(),
            )
            is Auth.Session -> {
                val cookie = sessionCookie ?: login(chain, a)
                val res = chain.proceed(chain.request().newBuilder().header("Cookie", cookie).build())
                if (res.code != 401) {
                    res
                } else {
                    res.close()
                    chain.proceed(chain.request().newBuilder().header("Cookie", login(chain, a)).build())
                }
            }
        }

        private fun login(chain: Interceptor.Chain, a: Auth.Session): String {
            val body = buildJsonObject { put("name", a.user); put("password", a.password) }
            val req = Request.Builder().url(server.newBuilder().addPathSegment("_session").build())
                .post(body.toString().toRequestBody(JSON)).build()
            chain.proceed(req).use { res ->
                if (!res.isSuccessful) throw AuthRequiredException("login failed: HTTP ${res.code}")
                val cookie = res.headers("Set-Cookie").firstOrNull { it.startsWith("AuthSession=") }
                    ?.substringBefore(';') ?: throw AuthRequiredException("no AuthSession cookie")
                sessionCookie = cookie
                return cookie
            }
        }
    }

    private companion object {
        val JSON = "application/json".toMediaType()
    }
}

private fun JsonObject.str(key: String) = (this[key] as? JsonPrimitive)?.contentOrNull

/** CouchDB 1.x used integer seqs; 2.x+ uses strings. Keep either as text. */
private fun JsonElement.seqString() = (this as? JsonPrimitive)?.content ?: toString()

private suspend fun Call.await(): Response = suspendCancellableCoroutine { cont ->
    cont.invokeOnCancellation { cancel() }
    enqueue(object : Callback {
        override fun onResponse(call: Call, response: Response) = cont.resume(response) { _, _, _ -> response.close() }
        override fun onFailure(call: Call, e: IOException) = cont.resumeWithException(e)
    })
}
