package org.couchdb.sync

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

/** sqlite-jdbc driver for JVM tests. */
class JdbcSqlDriver(url: String = "jdbc:sqlite::memory:") : SqlDriver {
    private val conn: Connection = DriverManager.getConnection(url)
    private var depth = 0

    override fun execute(sql: String, args: List<Any?>) {
        prepare(sql, args).use { it.execute() }
    }

    override fun <T> query(sql: String, args: List<Any?>, map: (SqlRow) -> T): List<T> =
        prepare(sql, args).use { st ->
            st.executeQuery().use { rs ->
                val row = object : SqlRow {
                    override fun string(index: Int) = rs.getString(index + 1)
                    override fun long(index: Int) = rs.getLong(index + 1).takeUnless { rs.wasNull() }
                }
                buildList { while (rs.next()) add(map(row)) }
            }
        }

    override fun <T> transaction(block: () -> T): T {
        if (depth++ == 0) conn.autoCommit = false
        try {
            val result = block()
            if (depth == 1) conn.commit()
            return result
        } catch (e: Throwable) {
            if (depth == 1) conn.rollback()
            throw e
        } finally {
            if (--depth == 0) conn.autoCommit = true
        }
    }

    private fun prepare(sql: String, args: List<Any?>): PreparedStatement =
        conn.prepareStatement(sql).apply { args.forEachIndexed { i, a -> setObject(i + 1, a) } }
}
