package org.couchdb.sync.android

import android.database.sqlite.SQLiteDatabase
import org.couchdb.sync.SqlDriver
import org.couchdb.sync.SqlRow

/** [SqlDriver] over the platform SQLite. */
class AndroidSqlDriver(private val db: SQLiteDatabase) : SqlDriver {
    override fun execute(sql: String, args: List<Any?>) = db.execSQL(sql, args.toTypedArray())

    override fun <T> query(sql: String, args: List<Any?>, map: (SqlRow) -> T): List<T> =
        // rawQuery only takes strings; SQLite converts them back for INTEGER columns.
        db.rawQuery(sql, args.map { it?.toString() }.toTypedArray()).use { c ->
            val row = object : SqlRow {
                override fun string(index: Int): String? = if (c.isNull(index)) null else c.getString(index)
                override fun long(index: Int): Long? = if (c.isNull(index)) null else c.getLong(index)
            }
            buildList { while (c.moveToNext()) add(map(row)) }
        }

    override fun <T> transaction(block: () -> T): T {
        db.beginTransaction() // nests natively
        try {
            return block().also { db.setTransactionSuccessful() }
        } finally {
            db.endTransaction()
        }
    }
}
