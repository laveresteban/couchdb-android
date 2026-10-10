package org.couchdb.sync.android

import android.database.Cursor
import org.couchdb.sync.SqlDriver
import org.couchdb.sync.SqlRow

/**
 * [SqlDriver] over either the platform SQLite or SQLCipher; both expose the
 * same calls, so the driver takes them as functions.
 */
class AndroidSqlDriver private constructor(
    private val exec: (String, Array<Any?>) -> Unit,
    private val rawQuery: (String, Array<String?>) -> Cursor,
    private val begin: () -> Unit,
    private val success: () -> Unit,
    private val end: () -> Unit,
) : SqlDriver {
    constructor(db: android.database.sqlite.SQLiteDatabase) :
        this(db::execSQL, db::rawQuery, db::beginTransaction, db::setTransactionSuccessful, db::endTransaction)

    constructor(db: net.zetetic.database.sqlcipher.SQLiteDatabase) :
        this(db::execSQL, { sql, args -> db.rawQuery(sql, args) }, db::beginTransaction, db::setTransactionSuccessful, db::endTransaction)

    override fun execute(sql: String, args: List<Any?>) = exec(sql, args.toTypedArray())

    override fun <T> query(sql: String, args: List<Any?>, map: (SqlRow) -> T): List<T> =
        // rawQuery only takes strings; SQLite converts them back for INTEGER/REAL columns.
        rawQuery(sql, args.map { it?.toString() }.toTypedArray()).use { c ->
            val row = object : SqlRow {
                override fun string(index: Int): String? = if (c.isNull(index)) null else c.getString(index)
                override fun long(index: Int): Long? = if (c.isNull(index)) null else c.getLong(index)
            }
            buildList { while (c.moveToNext()) add(map(row)) }
        }

    override fun <T> transaction(block: () -> T): T {
        begin() // nests natively
        try {
            return block().also { success() }
        } finally {
            end()
        }
    }
}
