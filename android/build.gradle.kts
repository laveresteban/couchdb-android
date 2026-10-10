plugins {
    id("com.android.library") version "8.7.3"
    kotlin("android")
}

android {
    namespace = "org.couchdb.sync.android"
    compileSdk = 35
    defaultConfig {
        minSdk = 24
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        // The emulator reaches the host's CouchDB at 10.0.2.2.
        testInstrumentationRunnerArguments["couchdbUrl"] = System.getenv("ANDROID_COUCHDB_URL") ?: "http://10.0.2.2:5984"
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
    implementation("net.zetetic:sqlcipher-android:4.6.1@aar")
    implementation("androidx.sqlite:sqlite:2.4.0")

    androidTestImplementation("androidx.test:runner:1.6.2")
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.work:work-testing:2.10.0")
    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
}
