pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        google()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        google()
    }
}

rootProject.name = "couchdb-android"
include(":sync-core")

// The Android module needs the Android SDK. Skip it when there isn't one so
// the core library and its tests still build on a plain JVM (and in CI's
// core job).
val hasAndroidSdk = System.getenv("ANDROID_HOME") != null ||
    file("local.properties").let { it.exists() && it.readText().contains("sdk.dir") }
if (hasAndroidSdk) include(":android")
