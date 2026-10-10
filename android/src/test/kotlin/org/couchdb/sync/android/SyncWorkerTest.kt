package org.couchdb.sync.android

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.work.ListenableWorker.Result
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.workDataOf
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.couchdb.sync.Auth
import org.couchdb.sync.ConflictResolver
import org.couchdb.sync.RemoteDb
import org.couchdb.sync.Resolution
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class SyncWorkerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val name = "worker-${UUID.randomUUID().toString().take(8)}"
    private val server = MockWebServer()
    private val routes = mutableMapOf<String, (RecordedRequest) -> MockResponse>()

    @Before
    fun start() {
        server.dispatcher =
            object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    val path = request.requestUrl!!.encodedPath
                    routes["${request.method} $path"]?.let { return it(request) }
                    if (request.method == "PUT" && path.contains("/_local/")) return json("""{"ok":true,"id":"x","rev":"0-1"}""", 201)
                    return json("""{"error":"not_found"}""", 404)
                }
            }
        server.start()
        routes["GET /$name"] = { json("{}") }
        routes["GET /$name/_changes"] = { json("""{"results":[],"last_seq":"0"}""") }
        routes["POST /$name/_revs_diff"] = { json("{}") }
    }

    @After
    fun stop() {
        server.shutdown()
        CouchLiteAndroid.destroy(context, name)
        CouchLiteAndroid.remotes = null
        CouchLiteAndroid.resolvers = null
    }

    private fun json(
        body: String,
        code: Int = 200,
    ) = MockResponse().setResponseCode(code).setHeader("Content-Type", "application/json").setBody(body)

    private fun remote(): RemoteDb = RemoteDb(server.url(name).toString(), Auth.Basic("u", "p"))

    private fun worker(vararg input: Pair<String, Any>) =
        TestListenableWorkerBuilder<SyncWorker>(context).setInputData(workDataOf(*input)).build()

    @Test
    fun `a job without a database name fails before touching the network`() =
        runBlocking<Unit> {
            val result = worker().doWork()
            assertIs<Result.Failure>(result)
            assertEquals("no_database", result.outputData.getString("error"))
        }

    @Test
    fun `an unreachable server asks WorkManager to retry`() =
        runBlocking<Unit> {
            CouchLiteAndroid.remotes = { RemoteDb("http://127.0.0.1:1/$it") }
            assertIs<Result.Retry>(worker(SyncWorker.KEY_DB to name).doWork())
        }

    @Test
    fun `a refused login fails with auth_required`() =
        runBlocking<Unit> {
            routes["GET /$name"] = { json("""{"error":"unauthorized"}""", 401) }
            CouchLiteAndroid.remotes = { remote() }
            val result = worker(SyncWorker.KEY_DB to name).doWork()
            assertIs<Result.Failure>(result)
            assertEquals("auth_required", result.outputData.getString("error"))
        }

    @Test
    fun `a forbidden database fails as an http error without retrying`() =
        runBlocking<Unit> {
            routes["GET /$name"] = { json("""{"error":"forbidden","reason":"no"}""", 403) }
            CouchLiteAndroid.remotes = { remote() }
            val result = worker(SyncWorker.KEY_DB to name).doWork()
            assertIs<Result.Failure>(result)
            assertEquals("http", result.outputData.getString("error"))
        }

    @Test
    fun `a successful run reports its counts`() =
        runBlocking<Unit> {
            CouchLiteAndroid.remotes = { remote() }
            val result = worker(SyncWorker.KEY_DB to name).doWork()
            assertIs<Result.Success>(result)
            assertEquals(0, result.outputData.getInt(SyncWorker.KEY_PULLED, -1))
            assertEquals(0, result.outputData.getInt(SyncWorker.KEY_PUSHED, -1))
        }

    @Test
    fun `a configured resolver settles conflicts from the background pull`() =
        runBlocking<Unit> {
            routes["GET /$name/_changes"] = {
                json("""{"results":[{"seq":"1","id":"n","changes":[{"rev":"2-b"},{"rev":"2-c"}]}],"last_seq":"1"}""")
            }
            routes["POST /$name/_bulk_get"] = {
                json(
                    """{"results":[{"id":"n","docs":[${doc("2-b", "B")},${doc("2-c", "C")}]}]}""",
                )
            }
            CouchLiteAndroid.remotes = { remote() }
            CouchLiteAndroid.resolvers = { ConflictResolver { _, others -> Resolution.Pick(others.first().rev) } }

            val result = worker(SyncWorker.KEY_DB to name).doWork()
            assertTrue(result is Result.Success, "${(result as? Result.Failure)?.outputData?.keyValueMap}")
            assertEquals(0, CouchLiteAndroid.open(context, name).conflicts("n").size)
        }

    @Test
    fun `unexpected errors fail the run with a code instead of crashing`() =
        runBlocking<Unit> {
            CouchLiteAndroid.remotes = null
            val result = worker(SyncWorker.KEY_DB to name).doWork()
            assertIs<Result.Failure>(result)
            assertEquals("unexpected", result.outputData.getString("error"))
        }

    private fun doc(
        rev: String,
        v: String,
    ): String {
        val hash = rev.substringAfter('-')
        return """{"ok":{"_id":"n","_rev":"$rev","_revisions":{"start":2,"ids":["$hash"]},"v":"$v"}}"""
    }
}
