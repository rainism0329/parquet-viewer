plugins {
    kotlin("jvm") version "1.9.23" // 或者 1.8.22 等你喜欢的版本
    id("java")
    id("org.jetbrains.intellij") version "1.17.2"
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(11))  // 使用 Java 17
}

intellij {
    version.set("2021.2")         // 指定 IntelliJ IDEA 的版本
    type.set("IU")                  // “IC” 表示 Community Edition
    plugins.set(listOf("java"))           // 可选：添加依赖插件，例如 "java", "Kotlin" 等
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
        sinceBuild.set("212")       // 最低兼容的 IntelliJ 版本
        untilBuild.set("241.*")     // 最高兼容的 IntelliJ 版本
    }

    runIde {

        jvmArgs = listOf("--add-opens=java.base/java.lang=ALL-UNNAMED")  // 有助于避免反射错误
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
