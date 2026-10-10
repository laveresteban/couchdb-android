package org.couchdb.sync.android

import android.database.sqlite.SQLiteDatabase
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class AndroidSqlDriverTest {
    private val driver = AndroidSqlDriver(SQLiteDatabase.create(null))

    @Test
    fun `binds and reads strings, integers and nulls`() {
        driver.execute("CREATE TABLE t (s TEXT, n INTEGER)")
        driver.execute("INSERT INTO t VALUES (?, ?)", listOf("x", 5L))
        driver.execute("INSERT INTO t VALUES (?, ?)", listOf(null, null))
        val rows = driver.query("SELECT s, n FROM t ORDER BY rowid") { it.string(0) to it.long(1) }
        assertEquals(listOf("x" to 5L, null to null), rows)
    }

    @Test
    fun `a transaction commits its work`() {
        driver.execute("CREATE TABLE t (v TEXT)")
        driver.transaction { driver.execute("INSERT INTO t VALUES ('a')") }
        assertEquals(1, driver.query("SELECT COUNT(*) FROM t") { it.long(0) }.single())
    }

    @Test
    fun `a transaction that throws rolls back`() {
        driver.execute("CREATE TABLE t (v TEXT)")
        assertFailsWith<IllegalStateException> {
            driver.transaction {
                driver.execute("INSERT INTO t VALUES ('a')")
                error("boom")
            }
        }
        assertEquals(0, driver.query("SELECT COUNT(*) FROM t") { it.long(0) }.single())
    }

    @Test
    fun `closing the driver closes the database`() {
        driver.execute("CREATE TABLE t (v TEXT)")
        driver.close()
        assertFailsWith<IllegalStateException> { driver.execute("SELECT 1") }
    }
}
