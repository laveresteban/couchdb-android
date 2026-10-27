plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    `java-test-fixtures`
    id("org.jetbrains.kotlinx.kover")
    id("com.vanniktech.maven.publish")
}

// Target Java 17 bytecode so Android (AGP 8) can consume the library.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
kotlin {
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
    api("com.squareup.okhttp3:okhttp:5.5.1")

    testFixturesImplementation("org.xerial:sqlite-jdbc:3.53.4.0")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("com.squareup.okhttp3:mockwebserver:5.5.1")
}

tasks.test {
    useJUnitPlatform()
    environment("COUCHDB_URL", System.getenv("COUCHDB_URL") ?: "http://localhost:5984")
}

kover {
    reports {
        verify {
            rule { minBound(88) }
        }
    }
}

mavenPublishing {
    publishToMavenCentral()
    if (project.hasProperty("signingInMemoryKey")) signAllPublications()
    coordinates(project.group.toString(), "sync-core", project.version.toString())
}
