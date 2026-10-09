plugins {
    id("com.android.library") version "8.7.3"
    kotlin("android")
}

android {
    namespace = "org.couchdb.sync.android"
    compileSdk = 35
    defaultConfig {
        minSdk = 24
        consumerProguardFiles("consumer-rules.pro")
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    api(project(":sync-core"))
    implementation("androidx.work:work-runtime-ktx:2.10.0")
}
