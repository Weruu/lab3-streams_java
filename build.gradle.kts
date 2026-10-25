plugins {
    java
}

group = "ua.kpi.comsys"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<JavaCompile>().configureEach {
    // Stream Gatherers (java.util.stream.Gatherer) стали стандартними у Java 24
    options.release = 24
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
