plugins {
    id("java")
    // 使用新版 IntelliJ Platform 插件
    id("org.jetbrains.intellij.platform") version "2.2.0"
}

repositories {
    mavenCentral()
    // 新版插件必须添加这个
    intellijPlatform {
        defaultRepositories()
    }
}

// *** 核心修复：配置 Java 工具链 ***
// 这会强制 Gradle 使用 JDK 21 进行编译，解决 "invalid source release: 21" 错误
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    // =======================================================
    // 1. 保留你原有的业务依赖 (Hadoop, Parquet, MVEL)
    // =======================================================
    implementation("org.apache.parquet:parquet-hadoop:1.13.1") {
        exclude(group = "org.slf4j")
        // The viewer uses Parquet's InputFile API and does not need the Hadoop client stack.
        exclude(group = "org.apache.hadoop")
    }
    implementation("org.mvel:mvel2:2.5.2.Final")
    // Keep native codecs current while retaining the proven Parquet 1.13 reader API.
    implementation("org.xerial.snappy:snappy-java:1.1.10.7")
    implementation("com.github.luben:zstd-jni:1.5.7-3")
    // Parquet exposes legacy Hadoop overloads in public reader signatures. They
    // are compile-time linkage only; the viewer supplies its own local InputFile
    // and lightweight codec factory at runtime.
    compileOnly("org.apache.hadoop:hadoop-common:3.3.6") {
        isTransitive = false
    }

    // 测试框架
    testImplementation("org.apache.parquet:parquet-avro:1.13.1") {
        exclude(group = "org.slf4j")
        exclude(group = "org.apache.hadoop")
    }
    // AvroParquetWriter exposes a legacy Hadoop Path overload in its public API;
    // compile against the type without putting Hadoop on the test or plugin runtime.
    testCompileOnly("org.apache.hadoop:hadoop-common:3.3.6") {
        isTransitive = false
    }
    testRuntimeOnly("org.apache.hadoop:hadoop-common:3.3.6") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }
    // ParquetWriter still links a legacy FileOutputFormat constructor even when
    // tests use the Hadoop-free OutputFile API. Keep that linkage test-only.
    testRuntimeOnly("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6") {
        isTransitive = false
    }
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.0")

    // =======================================================
    // 2. IntelliJ 平台配置 (适配 2024.2+)
    // =======================================================
    intellijPlatform {
        // 使用稳定的 IDEA 2024.2.4
        create("IC", "2024.2.4")

    }
}

intellijPlatform {
    pluginVerification {
        ides {
            ide("IC", "2024.2.4")
        }
    }

    pluginConfiguration {
        ideaVersion {
            // 兼容性范围设置
            sinceBuild = "242"
            untilBuild = provider { null }
        }

        changeNotes = """
            <b>1.4.1.3 - Viewer Experience and Performance Update</b><br/><br/>
            <ul>
                <li>Redesigned the tool window with multi-file tabs and native IntelliJ controls.</li>
                <li>Added cancellable background loading and bounded-memory virtualized Show All mode.</li>
                <li>Improved schema, metadata, column selection, filtering and streaming export.</li>
                <li>Reduced runtime dependencies and fixed stability issues.</li>
            </ul>
        """.trimIndent()
    }
}

tasks {
    test {
        useJUnitPlatform()
    }

    register<Test>("unitTest") {
        description = "Runs fast unit and local Parquet integration tests without an IDE sandbox."
        group = "verification"
        testClassesDirs = sourceSets.test.get().output.classesDirs
        classpath = sourceSets.test.get().runtimeClasspath
        useJUnitPlatform()
        dependsOn(testClasses)
        shouldRunAfter(test)
    }

    // 即使有了 toolchain，显式指定编译选项也是好习惯
    withType<JavaCompile> {
        sourceCompatibility = "21"
        targetCompatibility = "21"
        options.encoding = "UTF-8"
    }

}

group = "com.bigphil"
version = "1.4.1.3"
