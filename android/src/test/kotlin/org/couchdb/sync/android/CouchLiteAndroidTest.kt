package org.couchdb.sync.android

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.couchdb.sync.RemoteDb
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class CouchLiteAndroidTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val name = "android-${UUID.randomUUID().toString().take(8)}"

    private fun body(v: String) = JsonObject(mapOf("v" to JsonPrimitive(v)))

    @After
    fun tearDown() {
        CouchLiteAndroid.destroy(context, name)
        CouchLiteAndroid.remotes = null
        CouchLiteAndroid.filters = null
        CouchLiteAndroid.resolvers = null
    }

    @Test
    fun `open returns the same database until it is closed`() {
        val first = CouchLiteAndroid.open(context, name)
        assertSame(first, CouchLiteAndroid.open(context, name))
        CouchLiteAndroid.close(name)
        assertFalse(first === CouchLiteAndroid.open(context, name))
    }

    @Test
    fun `opening with a different encryption setting throws`() {
        CouchLiteAndroid.open(context, name)
        assertFailsWith<IllegalStateException> { CouchLiteAndroid.open(context, name, encrypted = true) }
    }

    @Test
    fun `data survives closing and reopening`() =
        runBlocking<Unit> {
            CouchLiteAndroid.open(context, name).put("n", body("kept"))
            CouchLiteAndroid.close(name)
            assertEquals("kept", CouchLiteAndroid.open(context, name).get("n")!!.body["v"]!!.jsonPrimitive.content)
        }

    @Test
    fun `closing a database that isn't open does nothing`() {
        CouchLiteAndroid.close(name)
        CouchLiteAndroid.close(name)
    }

    @Test
    fun `destroy deletes the database and its attachments`() =
        runBlocking<Unit> {
            val db = CouchLiteAndroid.open(context, name)
            val rev = db.put("n", body("1"))
            db.putAttachment("n", rev, "a.bin", "application/octet-stream", byteArrayOf(1, 2, 3))
            val blobDir = java.io.File(context.noBackupFilesDir, "couchlite/$name")
            assertEquals(true, blobDir.exists())

            CouchLiteAndroid.destroy(context, name)
            assertFalse(blobDir.exists())
            assertFalse(context.getDatabasePath("$name.couchlite.db").exists())
            val fresh = CouchLiteAndroid.open(context, name)
            assertNull(fresh.get("n"))
            assertNull(fresh.getAttachment("n", "a.bin"))
        }

    @Test
    fun `the remote needs a configured hook`() {
        assertFailsWith<IllegalStateException> { CouchLiteAndroid.remote(name) }
        CouchLiteAndroid.remotes = { RemoteDb("http://127.0.0.1:1/$it") }
        assertNotNull(CouchLiteAndroid.remote(name))
    }
}
