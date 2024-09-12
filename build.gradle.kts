plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

repositories {
    mavenCentral()
}

tasks {
    withType<JavaExec> {
        systemProperty("kotlinx.coroutines.debug", "")
    }

    named<CreateStartScripts>("startScripts") {
        defaultJvmOpts = listOf("-Dkotlinx.coroutines.debug")
    }
}

application {
    mainClass.set("dgroomes.MainKt")
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.slf4j.simple)
}
