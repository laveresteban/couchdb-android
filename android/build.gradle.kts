plugins {
    id("com.android.library") // version comes from the root buildscript
    kotlin("android")
    id("org.jetbrains.kotlinx.kover")
    id("com.vanniktech.maven.publish")
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
    testOptions {
        // Robolectric runs the unit tests on the JVM, with real resources.
        unitTests.isIncludeAndroidResources = true
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

    testImplementation("org.robolectric:robolectric:4.14.1")
    testImplementation("androidx.test:core:1.6.1")
    testImplementation("androidx.test.ext:junit:1.2.1")
    testImplementation("junit:junit:4.13.2")
    testImplementation(kotlin("test-junit"))
    testImplementation("androidx.work:work-testing:2.10.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")

    androidTestImplementation("androidx.test:runner:1.6.2")
    androidTestImplementation(kotlin("test-junit"))
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.work:work-testing:2.10.0")
    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
}

kover {
    reports {
        verify {
            rule { minBound(65) }
        }
    }
}

mavenPublishing {
    // Dokka (used for the AGP javadoc task) can't read sealed interfaces yet, so the javadoc jar is empty.
    configure(com.vanniktech.maven.publish.AndroidSingleVariantLibrary("release", sourcesJar = true, publishJavadocJar = false))
    publishToMavenCentral()
    if (project.hasProperty("signingInMemoryKey")) signAllPublications()
    coordinates(project.group.toString(), "couchdb-android", project.version.toString())
}

val emptyJavadocJar by tasks.registering(Jar::class) { archiveClassifier.set("javadoc") }
afterEvaluate {
    extensions.configure<org.gradle.api.publish.PublishingExtension> {
        publications.withType<org.gradle.api.publish.maven.MavenPublication>().configureEach { artifact(emptyJavadocJar) }
    }
}
