# Third-party notices

Parquet Viewer is licensed under Apache-2.0. Its dependencies remain under their
respective licenses; the project license does not replace their terms.

This document describes the dependencies resolved from `runtimeClasspath` on
2026-09-29. The [runtime inventory](third-party-licenses/RUNTIME_DEPENDENCIES.md)
records every resolved artifact and version, upstream license declarations, and
the license/notice entries retained inside each JAR. IntelliJ IDEA itself and
build/test tools are not part of this runtime inventory.

## License files in a plugin installation

Dependency JARs are distributed separately in the plugin's `lib` directory.
Their original license, notice, copyright, and embedded third-party license files
must be retained. The plugin's own JAR also contains:

- `META-INF/LICENSE` and `META-INF/NOTICE` for Parquet Viewer;
- `META-INF/THIRD_PARTY_NOTICES.md` (this document);
- `META-INF/third-party-licenses/` with the supplemental notices described below.

Some upstream Maven JARs omit standalone license or notice documents. The
supplemental directory supplies copies from their release sources and from
Apache Hadoop's release distribution. [SOURCES.json](third-party-licenses/SOURCES.json)
records each copied document's source and SHA-256 checksum. Upstream documents
are retained as published, including historical wording and spelling.

## Direct dependencies

| Component | Version | License | Upstream |
| --- | --- | --- | --- |
| Apache Hadoop (`hadoop-common`, `hadoop-mapreduce-client-core`) | 3.3.6 | Apache-2.0, with separately licensed embedded components | [Hadoop](https://github.com/apache/hadoop/tree/rel/release-3.3.6) |
| Apache Parquet (`parquet-avro`, `parquet-hadoop`, `parquet-column`, `parquet-common`) | 1.13.1 | Apache-2.0, with separately licensed embedded components | [Parquet](https://github.com/apache/parquet-mr/tree/apache-parquet-1.13.1) |
| MVEL | 2.5.2.Final | Apache-2.0 | [MVEL](https://github.com/mvel/mvel/tree/mvel2-2.5.2.Final) |
| Kotlin standard library (added by the Kotlin Gradle plugin) | 2.1.0 | Apache-2.0 | [Kotlin](https://github.com/JetBrains/kotlin/tree/v2.1.0) |

## Supplemental notices and embedded components

| Components | License information and local texts |
| --- | --- |
| Hadoop, and the transitive components described in its release | [Hadoop license bundle](third-party-licenses/hadoop-3.3.6/LICENSE-binary.txt) and [notices](third-party-licenses/hadoop-3.3.6/NOTICE-binary.txt). The `licenses-binary` subdirectory supplies notices for BSD/MIT components, Jersey/JAXB, and embedded resources. |
| Parquet | [License](third-party-licenses/parquet-1.13.1/LICENSE.txt) and [notices](third-party-licenses/parquet-1.13.1/NOTICE.txt), in addition to the documents retained in its JARs. |
| ZooKeeper 3.6.3 | [License](third-party-licenses/zookeeper-3.6.3/LICENSE.txt) and [notices](third-party-licenses/zookeeper-3.6.3/NOTICE.txt). |
| Netty 4.1.63.Final | Apache-2.0 with embedded code under additional permissive licenses; [license](third-party-licenses/netty-4.1.63.Final/LICENSE.txt), [notice](third-party-licenses/netty-4.1.63.Final/NOTICE.txt), and accompanying `license` directory. Netty 3.10.6.Final retains its own bundle inside its JAR. |
| Aircompressor 0.21 | [Apache-2.0](third-party-licenses/aircompressor-0.21/license.txt), with [Snappy BSD notices](third-party-licenses/aircompressor-0.21/notice.md). |
| Snappy Java 1.1.8.3 | [Apache-2.0](third-party-licenses/snappy-java-1.1.8.3/LICENSE.txt) and [notices](third-party-licenses/snappy-java-1.1.8.3/NOTICE.txt). The embedded Google Snappy BSD license is also retained in the Netty license bundle. |
| Zstd JNI 1.5.0-1 | [BSD-2-Clause for the JNI bindings](third-party-licenses/zstd-jni-1.5.0-1/LICENSE.txt); native Zstandard is available under [BSD-3-Clause](third-party-licenses/zstd-jni-1.5.0-1/src-main-native-LICENSE.txt), with its upstream alternative license also preserved. |
| Animal Sniffer annotations 1.17 | [MIT license and copyright notice](third-party-licenses/animal-sniffer-annotations-1.17/LICENSE.txt). |
| JSR-305 annotations 3.0.2 | Its POM declares Apache-2.0. Hadoop also records a BSD notice for the JSR-305 reference implementation. The embedded `javax.annotation.concurrent` annotations carry Brian Goetz's [copyright notice](third-party-licenses/jsr305-3.0.2/NOTICE.txt) and [CC-BY-2.5](third-party-licenses/jsr305-3.0.2/CC-BY-2.5.txt); these are preserved separately. |
| OkHttp 4.9.3 Public Suffix List | OkHttp is Apache-2.0. Its bundled Public Suffix List is [MPL-2.0](third-party-licenses/okhttp-4.9.3/MPL-2.0.txt), with the upstream [notice](third-party-licenses/okhttp-4.9.3/NOTICE.txt). Its source list and generator are available in the [matching OkHttp release](https://github.com/square/okhttp/tree/parent-4.9.3/okhttp/src/test). |
| Kotlin and MVEL | Supplemental upstream license documents are in `third-party-licenses/kotlin-2.1.0` and `third-party-licenses/mvel2-2.5.2.Final`. |

The Hadoop, Parquet, Kotlin, and Netty documents describe their upstream
distributions. They may mention optional modules, tools, or dependencies that
Parquet Viewer does not ship. Use the runtime inventory and the actual plugin
ZIP for the component list; inclusion of an upstream notice is not a statement
that every component mentioned in it is included in this plugin.

The remaining Apache-2.0 components include Avro, Commons libraries, Curator,
Kerby, HttpComponents, Jackson, Guava, Guice, Gson, Metrics, Okio, Reload4j,
Woodstox, Nimbus JOSE JWT, and various annotations. Other runtime components
include BSD-licensed dnsjava, JSch, Protocol Buffers, RE2/J, JLine, Stax2, and
Jakarta APIs; MIT-licensed Checker Framework qualifiers; and public-domain
AOP Alliance. Jetty offers Apache-2.0 or EPL-1.0 and is used here under its
Apache-2.0 option. Exact versions and upstream declarations are in the inventory.

## Jersey and Java EE API source availability

The unchanged Jersey, JAXB, servlet/JSP, and JAX-RS libraries are separate
third-party components. For dual-licensed components, this distribution uses the
CDDL option; their original license choices remain available. The upstream
[CDDL/GPL with Classpath Exception text](third-party-licenses/hadoop-3.3.6/licenses-binary/LICENSE-cddl-gplv2-ce.txt)
is included. This choice does not change Parquet Viewer's Apache-2.0 license.

Corresponding source archives, including their original copyright and license
headers, are publicly available from Maven Central:

| Component | Corresponding source |
| --- | --- |
| Jersey Client 1.19.4 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/jersey/jersey-client/1.19.4/jersey-client-1.19.4-sources.jar) |
| Jersey Core 1.19.4 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/jersey/jersey-core/1.19.4/jersey-core-1.19.4-sources.jar) |
| Jersey Server 1.19.4 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/jersey/jersey-server/1.19.4/jersey-server-1.19.4-sources.jar) |
| Jersey Servlet 1.19.4 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/jersey/jersey-servlet/1.19.4/jersey-servlet-1.19.4-sources.jar) |
| Jersey Guice 1.19.4 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/jersey/contribs/jersey-guice/1.19.4/jersey-guice-1.19.4-sources.jar) |
| Jersey JSON 1.20 | [Source JAR](https://repo.maven.apache.org/maven2/com/github/pjfanning/jersey-json/1.20/jersey-json-1.20-sources.jar) |
| JAXB implementation 2.2.3-1 | [Source JAR](https://repo.maven.apache.org/maven2/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1-sources.jar) |
| JAXB API 2.2.11 | [Source JAR](https://repo.maven.apache.org/maven2/javax/xml/bind/jaxb-api/2.2.11/jaxb-api-2.2.11-sources.jar) |
| Servlet API 3.1.0 | [Source JAR](https://repo.maven.apache.org/maven2/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0-sources.jar) |
| JSP API 2.1 | [Source JAR](https://repo.maven.apache.org/maven2/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1-sources.jar) |
| JAX-RS API 1.1.1 | [Source JAR](https://repo.maven.apache.org/maven2/javax/ws/rs/jsr311-api/1.1.1/jsr311-api-1.1.1-sources.jar) |

## Maintaining these notices

When dependencies change, resolve `runtimeClasspath` again, update the inventory
and supplemental notices against the exact upstream releases, verify the source
links, and inspect the built ZIP. Do not remove notices from dependency JARs or
assume that a Maven POM describes every embedded component. These notices cover
software dependencies; provenance for project artwork and screenshots must also
be retained by the maintainer.
