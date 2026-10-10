package org.couchdb.sync.example.notes

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import org.couchdb.sync.Auth
import org.couchdb.sync.RemoteDb

/** The local database name. The server database has the same name. */
const val DB_NAME = "notes"

/** Where the CouchDB server is and which account to sign in with. */
data class ServerConfig(val url: String, val user: String, val password: String) {
    /** The database on the server, for example `http://10.0.2.2:5984/notes`. */
    fun remote(database: String = DB_NAME): RemoteDb = RemoteDb("${url.trimEnd('/')}/$database", Auth.Session(user, password))
}

/** Server settings, encrypted at rest so the password isn't stored in plain text. */
class Settings(context: Context) {
    private val prefs: SharedPreferences =
        EncryptedSharedPreferences.create(
            context,
            "notes-settings",
            MasterKey.Builder(context).setKeyScheme(MasterKey.KeyScheme.AES256_GCM).build(),
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )

    /** Null until the app has connected to a server once. */
    var config: ServerConfig?
        get() {
            val url = prefs.getString(KEY_URL, null) ?: return null
            return ServerConfig(
                url = url,
                user = prefs.getString(KEY_USER, "").orEmpty(),
                password = prefs.getString(KEY_PASSWORD, "").orEmpty(),
            )
        }
        set(value) {
            val editor = prefs.edit()
            editor.clear()
            if (value != null) {
                editor.putString(KEY_URL, value.url)
                editor.putString(KEY_USER, value.user)
                editor.putString(KEY_PASSWORD, value.password)
            }
            editor.apply()
        }

    private companion object {
        const val KEY_URL = "url"
        const val KEY_USER = "user"
        const val KEY_PASSWORD = "password"
    }
}
