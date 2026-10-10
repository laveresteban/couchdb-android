plugins {
    id("com.android.application") // version comes from the root buildscript
    kotlin("android")
    id("org.jetbrains.kotlin.plugin.compose")
}

android {
    namespace = "org.couchdb.sync.example.notes"
    compileSdk = 35
    defaultConfig {
        applicationId = "org.couchdb.sync.example.notes"
        minSdk = 24
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"
    }
    buildFeatures { compose = true }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    // The SDK. Its sync-core dependency brings kotlinx-serialization for the JSON bodies.
    implementation(project(":android"))

    implementation(platform("androidx.compose:compose-bom:2024.12.01"))
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.ui:ui")

    implementation("androidx.activity:activity-compose:1.9.3")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.8.7")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.8.7")

    // Keeps the server URL and password encrypted at rest.
    implementation("androidx.security:security-crypto:1.1.0-alpha06")
}
