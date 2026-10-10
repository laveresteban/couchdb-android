package org.couchdb.sync

import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.RecordedRequest

/**
 * Canned CouchDB responses for protocol tests that run without a server.
 * Routes match `METHOD path` exactly, or `METHOD prefix` with [onPrefix].
 */
class FakeCouch : Dispatcher() {
    val requests = mutableListOf<RecordedRequest>()
    private val exact = mutableMapOf<String, (RecordedRequest) -> MockResponse>()
    private val prefixes = mutableListOf<Triple<String, String, (RecordedRequest) -> MockResponse>>()

    fun on(
        method: String,
        path: String,
        respond: (RecordedRequest) -> MockResponse,
    ) {
        exact["$method $path"] = respond
    }

    fun onPrefix(
        method: String,
        prefix: String,
        respond: (RecordedRequest) -> MockResponse,
    ) {
        prefixes += Triple(method, prefix, respond)
    }

    fun recorded(
        method: String,
        path: String,
    ) = requests.filter { it.method == method && it.requestUrl!!.encodedPath == path }

    /** Checkpoints start empty, and writes to them succeed. */
    fun withEmptyCheckpoints() {
        onPrefix("GET", "/db/_local/") { json("""{"error":"not_found"}""", 404) }
        onPrefix("PUT", "/db/_local/") { json("""{"ok":true,"id":"_local/x","rev":"0-1"}""", 201) }
    }

    override fun dispatch(request: RecordedRequest): MockResponse {
        synchronized(requests) { requests += request }
        val path = request.requestUrl!!.encodedPath
        exact["${request.method} $path"]?.let { return it(request) }
        prefixes.firstOrNull { (m, p, _) -> m == request.method && path.startsWith(p) }?.let { return it.third(request) }
        return json("""{"error":"not_found","reason":"no route ${request.method} $path"}""", 404)
    }

    companion object {
        fun json(
            body: String,
            code: Int = 200,
        ): MockResponse = MockResponse().setResponseCode(code).setHeader("Content-Type", "application/json").setBody(body)
    }
}
