package org.couchdb.conformance

import com.thoughtworks.gauge.AfterScenario
import com.thoughtworks.gauge.BeforeScenario
import com.thoughtworks.gauge.Step
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.Credentials
import okhttp3.HttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.couchdb.sync.Auth
import org.couchdb.sync.ChangesFeed
import org.couchdb.sync.CouchException
import org.couchdb.sync.RemoteDb
import org.couchdb.sync.RevRequest
import org.couchdb.sync.SyncFilter
import java.util.UUID

/**
 * Gauge steps for the shared conformance specs, implemented with the
 * SDK's [RemoteDb]. Covers the `documents`, `changes`, `sync` and
 * `databases` specs.
 */
class Steps {
    private val server = System.getenv("COUCHDB_URL") ?: "http://localhost:5984"
    private val user = System.getenv("COUCHDB_USER") ?: "admin"
    private val password = System.getenv("COUCHDB_PASSWORD") ?: "password"

    private lateinit var db: RemoteDb
    private lateinit var dbName: String
    private val created = mutableListOf<RemoteDb>()
    private val revs = mutableMapOf<String, String>()
    private var rememberedRev: String? = null
    private var rememberedSeq: String? = null
    private var createdId: String? = null

    @BeforeScenario
    fun reset() {
        revs.clear()
        rememberedRev = null
        rememberedSeq = null
        createdId = null
        sessionCookie = null
    }

    @AfterScenario
    fun cleanUp() =
        run {
            created.forEach { d -> runCatching { d.destroy() } }
            created.clear()
        }

    @Step("Connect as admin")
    fun connect() {
        val req =
            Request
                .Builder()
                .url("$server/_session")
                .header("Authorization", Credentials.basic(user, password))
                .build()
        val roles =
            OkHttpClient().newCall(req).execute().use {
                Json
                    .parseToJsonElement(it.body!!.string())
                    .jsonObject["userCtx"]!!
                    .jsonObject["roles"]!!
                    .jsonArray
            }
        check(roles.any { it.jsonPrimitive.content == "_admin" }) { "not admin on $server" }
    }

    @Step("Create a fresh database")
    fun createDatabase() =
        run {
            dbName = "conformance-${UUID.randomUUID().toString().take(8)}"
            db = RemoteDb("$server/$dbName", Auth.Basic(user, password))
            db.ensureExists(create = true)
            created += db
        }

    // ---- databases ----

    @Step("The database exists")
    fun dbExists() = run { check(db.request("GET") != null) }

    @Step("The database does not exist")
    fun dbMissing() = run { check(db.request("GET") == null) }

    @Step("Delete the database")
    fun deleteDb() = run { check(db.destroy()) }

    @Step("Creating the same database again fails with status <status>")
    fun createAgain(status: String) = run { expectStatus(status) { db.request("PUT") } }

    @Step("The database is listed among all databases")
    fun listed() {
        val req =
            Request
                .Builder()
                .url("$server/_all_dbs")
                .header("Authorization", Credentials.basic(user, password))
                .build()
        val names = OkHttpClient().newCall(req).execute().use { Json.parseToJsonElement(it.body!!.string()).jsonArray }
        check(names.any { it.jsonPrimitive.content == dbName }) { "$dbName not in $names" }
    }

    @Step("The database has <n> documents")
    fun docCount(n: String) =
        run {
            assertEq(
                n.toInt(),
                db
                    .request("GET")!!
                    .jsonObject["doc_count"]!!
                    .jsonPrimitive.content
                    .toInt(),
            )
        }

    // ---- documents ----

    @Step("Save document <id> with field <field> = <value>")
    fun save(
        id: String,
        field: String,
        value: String,
    ) = run {
        revs[id] = putDoc(id, buildJsonObject { put(field, value) })
    }

    @Step("Update document <id> setting field <field> = <value>")
    fun update(
        id: String,
        field: String,
        value: String,
    ) = run {
        val doc = getDoc(id)!!
        revs[id] = putDoc(id, JsonObject(doc + (field to JsonPrimitive(value))))
    }

    @Step("Delete document <id>")
    fun delete(id: String) =
        run {
            val rev = getDoc(id)!!["_rev"]!!.jsonPrimitive.content
            db.request("DELETE", listOf(id), mapOf("rev" to rev))
        }

    @Step("Document <id> has field <field> = <value>")
    fun hasField(
        id: String,
        field: String,
        value: String,
    ) = run {
        assertEq(value, getDoc(id)!![field]?.jsonPrimitive?.content)
    }

    @Step("Document <id> has a revision starting with <prefix>")
    fun revPrefix(
        id: String,
        prefix: String,
    ) = run {
        val rev = getDoc(id)!!["_rev"]!!.jsonPrimitive.content
        check(rev.startsWith(prefix)) { "$id is at $rev, expected $prefix..." }
    }

    @Step("Document <id> does not exist")
    fun missing(id: String) = run { check(getDoc(id) == null) { "$id still exists" } }

    @Step("Remember the revision of document <id>")
    fun rememberRev(id: String) = run { rememberedRev = getDoc(id)!!["_rev"]!!.jsonPrimitive.content }

    @Step("Saving document <id> with the remembered revision fails with status <status>")
    fun staleSave(
        id: String,
        status: String,
    ) = run {
        expectStatus(status) { putDoc(id, buildJsonObject { put("_rev", rememberedRev) }) }
    }

    @Step("Create a document without an id with field <field> = <value>")
    fun createWithoutId(
        field: String,
        value: String,
    ) = run {
        createdId =
            db
                .request("POST", body = buildJsonObject { put(field, value) })!!
                .jsonObject["id"]!!
                .jsonPrimitive.content
    }

    @Step("The created document can be read back with field <field> = <value>")
    fun readCreated(
        field: String,
        value: String,
    ) = run {
        assertEq(value, getDoc(createdId!!)!![field]?.jsonPrimitive?.content)
    }

    @Step("Bulk save <n> documents with field <field> = <value>")
    fun bulkSave(
        n: String,
        field: String,
        value: String,
    ) = run {
        val docs = JsonArray(List(n.toInt()) { buildJsonObject { put(field, value) } })
        val res = db.request("POST", listOf("_bulk_docs"), body = buildJsonObject { put("docs", docs) })!!.jsonArray
        check(res.none { it.jsonObject["error"] != null }) { "bulk errors: $res" }
    }

    @Step("All documents lists <n> rows")
    fun allDocs(n: String) =
        run {
            assertEq(
                n.toInt(),
                db
                    .request("POST", listOf("_all_docs"), body = JsonObject(emptyMap()))!!
                    .jsonObject["rows"]!!
                    .jsonArray.size,
            )
        }

    // ---- changes ----

    @Step("The changes feed lists documents <ids>")
    fun changesList(ids: String) = run { assertEq(ids.split(",").sorted(), changedIds("0")) }

    @Step("The changes feed lists no documents")
    fun changesEmpty() = run { assertEq(emptyList<String>(), changedIds("0")) }

    @Step("Remember the current update sequence")
    fun rememberSeq() =
        run {
            rememberedSeq =
                db
                    .request("GET")!!
                    .jsonObject["update_seq"]!!
                    .jsonPrimitive.content
        }

    @Step("The changes feed since the remembered sequence lists documents <ids>")
    fun changesSince(ids: String) = run { assertEq(ids.split(",").sorted(), changedIds(rememberedSeq!!)) }

    @Step("The changes feed marks document <id> as deleted")
    fun changesDeleted(id: String) =
        run {
            check(
                db
                    .changes("0", 1000)
                    .results
                    .single { it.id == id }
                    .deleted,
            ) { "$id not marked deleted" }
        }

    @Step("The changes feed filtered to documents <ids> lists documents <expected>")
    fun changesByIds(
        ids: String,
        expected: String,
    ) = run {
        val page = db.changes("0", 1000, filter = SyncFilter.DocIds(ids.split(",")))
        assertEq(expected.split(",").sorted(), page.results.map { it.id }.sorted())
    }

    @Step("The changes feed filtered by field <field> = <value> lists documents <expected>")
    fun changesBySelector(
        field: String,
        value: String,
        expected: String,
    ) = run {
        val page = db.changes("0", 1000, filter = SyncFilter.Selector(buildJsonObject { put(field, value) }))
        assertEq(expected.split(",").sorted(), page.results.map { it.id }.sorted())
    }

    @Step("The changes feed with all leaf revisions lists <n> revisions for document <id>")
    fun leafRevs(
        n: String,
        id: String,
    ) = run {
        assertEq(
            n.toInt(),
            db
                .changes("0", 1000)
                .results
                .single { it.id == id }
                .revs.size,
        )
    }

    @Step("Follow the changes feed with checkpoint <name> until document <id>")
    fun followUntil(
        name: String,
        id: String,
    ) = follow(name, id, continuous = false)

    @Step("Following the changes feed with checkpoint <name> next yields document <id>")
    fun followNext(
        name: String,
        id: String,
    ) = next(name, id, continuous = false)

    @Step("Follow the continuous changes feed with checkpoint <name> until document <id>")
    fun followContinuousUntil(
        name: String,
        id: String,
    ) = follow(name, id, continuous = true)

    @Step("Following the continuous changes feed with checkpoint <name> next yields document <id>")
    fun followContinuousNext(
        name: String,
        id: String,
    ) = next(name, id, continuous = true)

    private fun follow(
        name: String,
        id: String,
        continuous: Boolean,
    ) = run {
        val feed = ChangesFeed(db, checkpoint = name, continuous = continuous)
        withTimeout(15_000) { feed.asFlow().collect { if (it.id == id) feed.stop() } }
    }

    private fun next(
        name: String,
        id: String,
        continuous: Boolean,
    ) = run {
        val first = withTimeout(15_000) { ChangesFeed(db, checkpoint = name, continuous = continuous).asFlow().first() }
        assertEq(id, first.id)
    }

    // ---- sync primitives ----

    @Step("Revs diff for document <id> with the remembered revision reports nothing missing")
    fun revsDiffNone(id: String) =
        run {
            assertEq(emptyList<String>(), db.revsDiff(mapOf(id to listOf(rememberedRev!!)))[id]?.missing.orEmpty())
        }

    @Step("Revs diff for document <id> with revision <rev> reports <missing> missing")
    fun revsDiffMissing(
        id: String,
        rev: String,
        missing: String,
    ) = run {
        assertEq(listOf(missing), db.revsDiff(mapOf(id to listOf(rev)))[id]?.missing)
    }

    @Step("Bulk get documents <ids> with history returns <n> documents")
    fun bulkGet(
        ids: String,
        n: String,
    ) = run {
        val res = db.bulkGet(ids.split(",").map { RevRequest(it) })
        assertEq(n.toInt(), res.count { it.doc?.get("_revisions") != null })
    }

    @Step("Bulk get history of document <id> lists <n> revisions")
    fun bulkGetHistory(
        id: String,
        n: String,
    ) = run {
        val doc = db.bulkGet(listOf(RevRequest(id))).single().doc!!
        assertEq(n.toInt(), doc["_revisions"]!!.jsonObject["ids"]!!.jsonArray.size)
    }

    @Step("Bulk get of missing document <id> reports <error>")
    fun bulkGetMissing(
        id: String,
        error: String,
    ) = run {
        assertEq(error, db.bulkGet(listOf(RevRequest(id))).single().error)
    }

    @Step("Save local document <id> with field <field> = <value>")
    fun saveLocal(
        id: String,
        field: String,
        value: String,
    ) = run {
        db.putLocal(id, buildJsonObject { put(field, value) })
    }

    @Step("Local document <id> has field <field> = <value>")
    fun localField(
        id: String,
        field: String,
        value: String,
    ) = run {
        assertEq(value, db.getLocal(id)!![field]?.jsonPrimitive?.content)
    }

    @Step("Delete local document <id>")
    fun deleteLocal(id: String) =
        run {
            val rev = db.getLocal(id)!!["_rev"]!!.jsonPrimitive.content
            db.request("DELETE", listOf("_local", id), mapOf("rev" to rev))
        }

    @Step("Local document <id> does not exist")
    fun localMissing(id: String) = run { check(db.getLocal(id) == null) }

    @Step("Write document <id> at revision <rev> with field <field> = <value> without new edits")
    fun writeNoNewEdits(
        id: String,
        rev: String,
        field: String,
        value: String,
    ) = run {
        db.bulkDocsNoNewEdits(
            listOf(
                buildJsonObject {
                    put("_id", id)
                    put("_rev", rev)
                    put(field, value)
                },
            ),
        )
    }

    @Step("Document <id> has <n> conflicts")
    fun conflicts(
        id: String,
        n: String,
    ) = run {
        val doc = db.request("GET", listOf(id), mapOf("conflicts" to "true"))!!.jsonObject
        assertEq(n.toInt(), doc["_conflicts"]?.jsonArray?.size ?: 0)
    }

    @Step("Document <id> with revision history lists <n> revisions")
    fun history(
        id: String,
        n: String,
    ) = run {
        val doc = db.request("GET", listOf(id), mapOf("revs" to "true"))!!.jsonObject
        assertEq(n.toInt(), doc["_revisions"]!!.jsonObject["ids"]!!.jsonArray.size)
    }

    // ---- attachments ----

    @Step("Attach <content> as <name> to document <id>")
    fun attach(
        content: String,
        name: String,
        id: String,
    ) = run { attachBytes(id, name, content.toByteArray(), "text/plain") }

    @Step("Attach <content> as <name> with content type <type> to document <id>")
    fun attachTyped(
        content: String,
        name: String,
        type: String,
        id: String,
    ) = run {
        attachBytes(id, name, content.toByteArray(), type)
    }

    @Step("Attachment <name> of document <id> contains <content>")
    fun attachmentContains(
        name: String,
        id: String,
        content: String,
    ) = run {
        val (code, bytes) = rawCall("GET", docUrl(id, name), null)
        assertEq(200, code)
        assertEq(content, String(bytes))
    }

    @Step("Document <id> lists attachment <name>")
    fun listsAttachment(
        id: String,
        name: String,
    ) = run {
        check(getDoc(id)!!["_attachments"]?.jsonObject?.containsKey(name) == true) { "$id has no attachment $name" }
    }

    @Step("Delete attachment <name> from document <id>")
    fun deleteAttachment(
        name: String,
        id: String,
    ) = run {
        val rev = getDoc(id)!!["_rev"]!!.jsonPrimitive.content
        val (code, _) = rawCall("DELETE", docUrl(id, name).newBuilder().addQueryParameter("rev", rev).build(), null)
        check(code == 200 || code == 202) { "delete returned $code" }
    }

    @Step("Attachment <name> of document <id> does not exist")
    fun attachmentMissing(
        name: String,
        id: String,
    ) = run { assertEq(404, rawCall("GET", docUrl(id, name), null).first) }

    @Step("Attachment <name> of document <id> has content type <type>")
    fun attachmentType(
        name: String,
        id: String,
        type: String,
    ) = run {
        val info = getDoc(id)!!["_attachments"]!!.jsonObject[name]!!.jsonObject
        assertEq(type, info["content_type"]!!.jsonPrimitive.content)
    }

    // ---- authentication ----

    private var sessionCookie: String? = null

    @Step("Connect anonymously")
    fun connectAnonymously() = run { assertEq(null, sessionUser(null)) }

    @Step("Log in with the admin credentials")
    fun logIn() =
        run {
            val (code, cookie) = login(password)
            check(code == 200 && cookie != null) { "login failed: $code" }
            sessionCookie = cookie
        }

    @Step("The session user is the admin user")
    fun sessionIsAdmin() = run { assertEq(user, sessionUser(sessionCookie)) }

    @Step("Log out")
    fun logOut() =
        run {
            val req =
                Request
                    .Builder()
                    .url("$server/_session")
                    .delete()
                    .header("Cookie", sessionCookie!!)
                    .build()
            OkHttpClient().newCall(req).execute().use { check(it.isSuccessful) { "logout failed: ${it.code}" } }
            // CouchDB keeps honoring a cookie until it expires, so a logged-out client sends none.
            sessionCookie = null
        }

    @Step("The session user is anonymous")
    fun sessionIsAnonymous() = run { assertEq(null, sessionUser(sessionCookie)) }

    @Step("Logging in with password <attempt> fails with status <status>")
    fun badLogin(
        attempt: String,
        status: String,
    ) = run { assertEq(status.toInt(), login(attempt).first) }

    // ---- helpers ----

    private fun docUrl(vararg segments: String): HttpUrl =
        db.url
            .newBuilder()
            .apply { segments.forEach(::addPathSegment) }
            .build()

    private fun rawCall(
        method: String,
        url: HttpUrl,
        body: RequestBody?,
    ): Pair<Int, ByteArray> {
        val req =
            Request
                .Builder()
                .url(url)
                .header("Authorization", Credentials.basic(user, password))
                .method(method, body)
                .build()
        return OkHttpClient().newCall(req).execute().use { it.code to it.body!!.bytes() }
    }

    private suspend fun attachBytes(
        id: String,
        name: String,
        data: ByteArray,
        type: String,
    ) {
        val rev =
            db
                .request("GET", listOf(id))!!
                .jsonObject["_rev"]!!
                .jsonPrimitive.content
        val url = docUrl(id, name).newBuilder().addQueryParameter("rev", rev).build()
        val (code, body) = rawCall("PUT", url, data.toRequestBody(type.toMediaType()))
        check(code == 201) { "attach returned $code: ${String(body)}" }
    }

    /** Posts [password] for the admin user to `_session`. Returns the status and the AuthSession cookie. */
    private fun login(password: String): Pair<Int, String?> {
        val body =
            buildJsonObject {
                put("name", user)
                put("password", password)
            }
        val req =
            Request
                .Builder()
                .url("$server/_session")
                .post(body.toString().toRequestBody("application/json".toMediaType()))
                .build()
        return OkHttpClient().newCall(req).execute().use { res ->
            res.code to res.headers("Set-Cookie").firstOrNull { it.startsWith("AuthSession=") }?.substringBefore(';')
        }
    }

    /** The user name `_session` reports for [cookie] (null when anonymous). */
    private fun sessionUser(cookie: String?): String? {
        val req =
            Request
                .Builder()
                .url("$server/_session")
                .apply { cookie?.let { header("Cookie", it) } }
                .build()
        return OkHttpClient().newCall(req).execute().use { res ->
            Json
                .parseToJsonElement(res.body!!.string())
                .jsonObject["userCtx"]!!
                .jsonObject["name"]
                ?.takeUnless { it is JsonNull }
                ?.jsonPrimitive
                ?.content
        }
    }

    private suspend fun getDoc(id: String): JsonObject? = db.request("GET", listOf(id))?.jsonObject

    private suspend fun putDoc(
        id: String,
        body: JsonObject,
    ): String =
        db
            .request("PUT", listOf(id), body = body)!!
            .jsonObject["rev"]!!
            .jsonPrimitive.content

    private suspend fun changedIds(since: String) =
        db
            .changes(since, 1000)
            .results
            .map { it.id }
            .sorted()

    private suspend fun expectStatus(
        status: String,
        block: suspend () -> Any?,
    ) {
        val e = runCatching { block() }.exceptionOrNull()
        check(e is CouchException && e.status == status.toInt()) { "expected HTTP $status, got ${e ?: "success"}" }
    }

    private fun assertEq(
        expected: Any?,
        actual: Any?,
    ) = check(expected == actual) { "expected $expected but was $actual" }

    private fun <T> run(block: suspend () -> T): Unit = runBlocking { block() }.let { }
}
