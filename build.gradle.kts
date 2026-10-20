buildscript {
    // AGP has to be on the same classloader as the Kotlin plugin (the root
    // one), or kotlin("android") can't see AGP's classes. Only add it when
    // the Android module is in the build, so JVM-only builds don't need
    // Google's Maven repository.
    if (gradle.extra["hasAndroidSdk"] == true) {
        repositories {
            google()
            mavenCentral()
        }
        dependencies { classpath("com.android.tools.build:gradle:8.7.3") }
    }
}

plugins {
    kotlin("jvm") version "2.4.21" apply false
    kotlin("plugin.serialization") version "2.4.21" apply false
    kotlin("plugin.compose") version "2.4.21" apply false // example-notes uses Jetpack Compose
    id("org.jetbrains.kotlinx.kover") version "0.9.11" apply false
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0" apply false
    id("com.vanniktech.maven.publish") version "0.37.0" apply false
}

// Every module is linted with the same ktlint rules.
subprojects {
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
}

allprojects {
    group = "io.github.laveresteban"
    version = "0.1.0"
}
