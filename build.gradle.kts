plugins {
    kotlin("jvm") version "1.9.23"
    id("java")
    id("org.jetbrains.intellij") version "1.17.2"
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(11))
}

intellij {
    version.set("2021.2")
    type.set("IC")
    plugins.set(listOf("java"))
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.3.6") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.parquet:parquet-avro:1.13.1") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.parquet:parquet-hadoop:1.13.1") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.parquet:parquet-column:1.13.1") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.parquet:parquet-common:1.13.1") {
        exclude(group = "org.slf4j")
    }

    // JUnit 测试框架
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.0")
}


tasks {
    patchPluginXml {
        sinceBuild.set("212")
        untilBuild.set("241.*")
    }

    runIde {

        jvmArgs = listOf("--add-opens=java.base/java.lang=ALL-UNNAMED")
    }
}

repositories {
    maven { url = uri("https://maven.aliyun.com/repository/gradle-plugin") }
    maven { url = uri("https://maven.aliyun.com/repository/public") }
    mavenCentral()
}

tasks.test {
    useJUnitPlatform()
}
