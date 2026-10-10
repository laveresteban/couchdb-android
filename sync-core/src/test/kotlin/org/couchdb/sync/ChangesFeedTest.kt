package org.couchdb.sync

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import java.io.IOException
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ChangesFeedTest {
    private val server = System.getenv("COUCHDB_URL") ?: "http://localhost:5984"
    private val db = RemoteDb("$server/feed-test-${UUID.randomUUID().toString().take(8)}", Auth.Basic("admin", "password"))

    @Test
    fun `transient errors are retried with backoff, then give up`() = runBlocking {
        val down = RemoteDb("http://127.0.0.1:1/nothing")
        val sleeps = mutableListOf<Long>()
        val feed = ChangesFeed(down, maxRetries = 3, maxBackoffMs = 4_000, sleep = { sleeps += it })
        assertFailsWith<IOException> { feed.asFlow().first() }
        assertEquals(3, sleeps.size)
        assertTrue(sleeps.all { it in 250..4_000 }, "$sleeps")
    }

    @Test
    fun `only network errors, 429 and 5xx are transient`() {
        assertTrue(ChangesFeed.isTransient(IOException("reset")))
        assertTrue(ChangesFeed.isTransient(CouchException(503, null, null)))
        assertTrue(ChangesFeed.isTransient(CouchException(429, null, null)))
        assertFalse(ChangesFeed.isTransient(CouchException(404, null, null)))
        assertFalse(ChangesFeed.isTransient(AuthRequiredException(null)))
    }

    @BeforeEach
    fun setUp(info: org.junit.jupiter.api.TestInfo) {
        if (info.displayName.contains("retried") || info.displayName.contains("transient")) return
        val up = runCatching { OkHttpClient().newCall(Request.Builder().url("$server/_up").build()).execute().use { it.isSuccessful } }
        assumeTrue(up.getOrDefault(false), "CouchDB not reachable at $server")
        runBlocking { db.ensureExists(create = true) }
    }

    @AfterEach
    fun tearDown() {
        runBlocking { runCatching { db.destroy() } }
    }

    private suspend fun save(id: String) = db.request("PUT", listOf(id), body = buildJsonObject { put("v", 1) })

    private suspend fun readUntil(feed: ChangesFeed, id: String) = withTimeout(10_000) {
        feed.asFlow().collect { if (it.id == id) feed.stop() }
    }

    @Test
    fun `longpoll reader resumes from its checkpoint`() = runBlocking {
        save("a")
        readUntil(ChangesFeed(db, checkpoint = "r"), "a")
        save("c")
        assertEquals("c", withTimeout(10_000) { ChangesFeed(db, checkpoint = "r").asFlow().first().id })
    }

    @Test
    fun `continuous reader resumes from its checkpoint`() = runBlocking {
        save("a")
        readUntil(ChangesFeed(db, checkpoint = "c", continuous = true), "a")
        save("c")
        assertEquals("c", withTimeout(10_000) { ChangesFeed(db, checkpoint = "c", continuous = true).asFlow().first().id })
    }

    @Test
    fun `continuous feed delivers changes made while connected`() = runBlocking {
        val feed = ChangesFeed(db, since = "now", continuous = true)
        val seen = kotlinx.coroutines.CompletableDeferred<String>()
        val job = launch(kotlinx.coroutines.Dispatchers.IO) { feed.asFlow().collect { seen.complete(it.id); feed.stop() } }
        kotlinx.coroutines.delay(500)
        save("live")
        assertEquals("live", withTimeout(10_000) { seen.await() })
        withTimeout(10_000) { job.join() }
    }
}
