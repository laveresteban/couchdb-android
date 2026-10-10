package org.couchdb.sync.android

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.ListenableWorker
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.workDataOf
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.couchdb.sync.Auth
import org.couchdb.sync.Query
import org.couchdb.sync.RemoteDb
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

/** Runs on an emulator or device; CI points it at CouchDB on the host. */
@RunWith(AndroidJUnit4::class)
class DeviceTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val couch = InstrumentationRegistry.getArguments().getString("couchdbUrl") ?: "http://10.0.2.2:5984"
    private val name = "dev-${UUID.randomUUID().toString().take(8)}"
    private val remote = RemoteDb("$couch/$name", Auth.Basic("admin", "password"))

    @After
    fun tearDown() = runBlocking {
        CouchLiteAndroid.destroy(context, name)
        runCatching { remote.destroy() }
        Unit
    }

    @Test
    fun platformSqliteStoresAndQueries() = runBlocking {
        val db = CouchLiteAndroid.open(context, name)
        db.createIndex("n")
        repeat(20) { db.put("d$it", buildJsonObject { put("n", it) }) }
        val hits = db.find(Query(buildJsonObject { put("n", buildJsonObject { put("\$gte", 15) }) }))
        assertEquals(5, hits.size)
    }

    @Test
    fun encryptedDatabaseReopensAndHidesData() = runBlocking {
        val db = CouchLiteAndroid.open(context, name, encrypted = true)
        val rev = db.put("secret", buildJsonObject { put("pin", "4321") })
        db.putAttachment("secret", rev, "scan.bin", "application/octet-stream", "attachment-plaintext".toByteArray())

        val file = context.getDatabasePath("$name.couchlite.db")
        assertFalse(String(file.readBytes(), Charsets.ISO_8859_1).contains("4321"))
        val blobs = java.io.File(context.noBackupFilesDir, "couchlite/$name").listFiles()!!
        assertTrue(blobs.none { String(it.readBytes(), Charsets.ISO_8859_1).contains("attachment-plaintext") })

        assertEquals("4321", db.get("secret")!!.body["pin"]!!.jsonPrimitive.content)
        assertArrayEquals("attachment-plaintext".toByteArray(), db.getAttachment("secret", "scan.bin"))
    }

    @Test
    fun workerSyncsWithServer() = runBlocking {
        remote.ensureExists(create = true)
        CouchLiteAndroid.remotes = { RemoteDb("$couch/$it", Auth.Basic("admin", "password")) }
        val db = CouchLiteAndroid.open(context, name)
        db.put("from-phone", buildJsonObject { put("v", 1) })

        val worker = TestListenableWorkerBuilder<SyncWorker>(context)
            .setInputData(workDataOf("db" to name))
            .build()
        val result = worker.doWork()
        assertTrue(result is ListenableWorker.Result.Success)
        assertEquals(1, result.outputData.getInt("pushed", -1))
        assertTrue(remote.request("GET", listOf("from-phone")) != null)
    }
}
