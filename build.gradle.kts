import org.jetbrains.intellij.platform.gradle.TestFrameworkType

plugins {
    id("java")
    // 升级 Kotlin 以适配新版 IDEA
    id("org.jetbrains.kotlin.jvm") version "2.1.0"
    // 使用新版 IntelliJ Platform 插件
    id("org.jetbrains.intellij.platform") version "2.2.0"
}

version = "2.5.2.2"

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

// Keep the existing test directory usable on case-sensitive filesystems too.
sourceSets.test {
    java.setSrcDirs(listOf("src/Test/java"))
}

dependencies {
    // =======================================================
    // 1. 保留你原有的业务依赖 (Hadoop, Parquet, MVEL)
    // =======================================================
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
    implementation("org.mvel:mvel2:2.5.2.Final")

    // 测试框架
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.0")
    // IntelliJ's 2024.2 test session listener still references JUnit 4 classes.
    testRuntimeOnly("junit:junit:4.13.2")

    // =======================================================
    // 2. IntelliJ 平台配置 (适配 2024.2+)
    // =======================================================
    intellijPlatform {
        // 使用稳定的 IDEA 2024.2.4
        create("IC", "2024.2.4")

        testFramework(TestFrameworkType.Platform)

        // 必须包含 java 插件
        bundledPlugin("com.intellij.java")
    }
}

intellijPlatform {
    pluginConfiguration {
        ideaVersion {
            // 兼容性范围设置
            sinceBuild = "242"
            untilBuild = provider { null }
        }

        changeNotes = """
            <b>2.5.2.2 - Open-source licensing</b><br/>
            <ul>
                <li>Document the project's Apache License 2.0 and link to the source code.</li>
                <li>Include license and third-party notices in the distribution.</li>
                <li>Add build and contribution instructions.</li>
            </ul>
            <b>2.5.2 - Quality of Life Update</b><br/><br/>
            <ul>
                <li>🔍 <b>Filter History:</b> Your last 20 filter expressions are saved and available from the dropdown (▾) next to the filter bar. Persists across IDE restarts.</li>
                <li>💬 <b>Filter Error Feedback:</b> Invalid filter expressions now show a clear error message instead of failing silently.</li>
                <li>📏 <b>Column Width Persistence:</b> Manually resized column widths now survive page changes and filter refreshes.</li>
                <li>🔧 <b>Special Character Column Names:</b> Columns with hyphens, dots, or Unicode characters now work in LIKE and IN filters.</li>
                <li>🏗️ <b>Recursive Code Generation:</b> Hive DDL and Java POJO now fully expand nested STRUCT types.</li>
                <li>🐛 <b>Bug Fixes:</b> Fixed MVEL expression compilation in IDE classloader. Various stability improvements.</li>
            </ul>
        """.trimIndent()
    }
}

tasks {
    processResources {
        from(files("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md")) {
            into("META-INF")
        }
        from("third-party-licenses") {
            into("META-INF/third-party-licenses")
        }
    }

    test {
        useJUnitPlatform()
    }

    // 即使有了 toolchain，显式指定编译选项也是好习惯
    withType<JavaCompile> {
        sourceCompatibility = "21"
        targetCompatibility = "21"
        options.encoding = "UTF-8"
    }

    withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        }
    }
}
