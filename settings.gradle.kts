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
include(":sync-core", ":conformance")

// The Android modules need the Android SDK. Skip them when there isn't one so
// the core library and its tests still build on a plain JVM (and in CI's
// core job).
val hasAndroidSdk = !System.getenv("ANDROID_HOME").isNullOrBlank() ||
    file("local.properties").let { it.exists() && it.readText().contains("sdk.dir") }
if (hasAndroidSdk) include(":android", ":example-notes")
gradle.extra["hasAndroidSdk"] = hasAndroidSdk
