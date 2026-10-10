package org.couchdb.sync

/**
 * The few SQLite calls the store needs. Android wraps `SQLiteDatabase`;
 * tests wrap sqlite-jdbc. Implementations don't need to be thread safe:
 * [SqlLocalStore] serializes access.
 */
interface SqlDriver {
    fun execute(
        sql: String,
        args: List<Any?> = emptyList(),
    )

    fun <T> query(
        sql: String,
        args: List<Any?> = emptyList(),
        map: (SqlRow) -> T,
    ): List<T>

    /** Runs [block] in a transaction. Nested calls join the outer transaction. */
    fun <T> transaction(block: () -> T): T

    fun close()
}

interface SqlRow {
    fun string(index: Int): String?

    fun long(index: Int): Long?
}
