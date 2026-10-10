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
    runtimeOnly("org.xerial:sqlite-jdbc:3.46.1.3")
    implementation("com.thoughtworks.gauge:gauge-java:1.0.3")
}

// Runs the shared specs (copied in by scripts/conformance.sh) and the SDK
// specs in sdk-specs/, with the compiled step classes on the classpath.
tasks.register<Exec>("gauge") {
    dependsOn("classes", configurations.runtimeClasspath)
    workingDir = projectDir
    val tags = (project.findProperty("tags") as String?) ?: "documents | changes | sync | databases | attachments | auth | sdk"
    commandLine("gauge", "run", "--tags", tags, "specs", "sdk-specs")
    doFirst {
        val cp = sourceSets["main"].runtimeClasspath.files.joinToString(File.pathSeparator)
        environment("gauge_custom_classpath", cp)
    }
}
