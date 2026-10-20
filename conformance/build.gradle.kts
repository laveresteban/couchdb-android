plugins {
    kotlin("jvm")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
kotlin {
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

dependencies {
    implementation(project(":sync-core"))
    implementation(testFixtures(project(":sync-core")))
    runtimeOnly("org.xerial:sqlite-jdbc:3.53.4.0")
    implementation("com.thoughtworks.gauge:gauge-java:1.0.3")
}

// Runs the shared specs (copied in by scripts/conformance.sh) and the SDK
// specs in sdk-specs/, with the compiled step classes on the classpath.
tasks.register<Exec>("gauge") {
    // The classpath names jar files, so build them (classes alone don't).
    dependsOn("classes", project(":sync-core").tasks.named("jar"), project(":sync-core").tasks.named("testFixturesJar"))
    workingDir = projectDir
    val tags = (project.findProperty("tags") as String?) ?: "documents | changes | sync | databases | attachments | auth | sdk"
    commandLine("gauge", "run", "--tags", tags, "specs", "sdk-specs")
    environment("gauge_custom_classpath", sourceSets.named("main").map { it.runtimeClasspath.asPath })
}
