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
    implementation("com.thoughtworks.gauge:gauge-java:1.0.3")
}

// Runs the shared Gauge specs (copied in by scripts/conformance.sh) with
// the compiled step classes on the classpath.
tasks.register<Exec>("gauge") {
    dependsOn("classes")
    workingDir = projectDir
    val tags = (project.findProperty("tags") as String?) ?: "documents | changes | sync | databases"
    commandLine("gauge", "run", "--tags", tags, "specs")
    doFirst {
        val cp = sourceSets["main"].runtimeClasspath.files.joinToString(File.pathSeparator)
        environment("gauge_custom_classpath", cp)
    }
}
