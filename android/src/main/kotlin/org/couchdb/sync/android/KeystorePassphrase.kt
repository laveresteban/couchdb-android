package org.couchdb.sync.android

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * A random 32-byte secret per database, stored in SharedPreferences
 * encrypted with an AES key that never leaves the Android Keystore. It's
 * used as the SQLCipher passphrase and the attachment encryption key.
 *
 * If the Keystore key is lost (app data cleared, some device restores),
 * the secret can't be decrypted and the local database must be recreated
 * and synced again; it's a cache of the server, so nothing is lost there.
 */
internal object KeystorePassphrase {
    private const val PREFS = "couchlite.keys"
    private const val KEY_ALIAS = "couchlite.master"

    fun get(context: Context, name: String): ByteArray {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        prefs.getString(name, null)?.let { return decrypt(Base64.decode(it, Base64.NO_WRAP)) }
        val secret = ByteArray(32).also(SecureRandom()::nextBytes)
        prefs.edit().putString(name, Base64.encodeToString(encrypt(secret), Base64.NO_WRAP)).commit()
        return secret
    }

    fun forget(context: Context, name: String) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().remove(name).commit()
    }

    private fun masterKey(): SecretKey {
        val ks = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        (ks.getKey(KEY_ALIAS, null) as? SecretKey)?.let { return it }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").apply {
            init(
                KeyGenParameterSpec.Builder(KEY_ALIAS, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                    .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .setKeySize(256)
                    .build(),
            )
        }.generateKey()
    }

    private fun encrypt(data: ByteArray): ByteArray {
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.ENCRYPT_MODE, masterKey()) // Keystore picks the IV
        return c.iv + c.doFinal(data)
    }

    private fun decrypt(data: ByteArray): ByteArray {
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.DECRYPT_MODE, masterKey(), GCMParameterSpec(128, data, 0, 12))
        return c.doFinal(data, 12, data.size - 12)
    }
}
