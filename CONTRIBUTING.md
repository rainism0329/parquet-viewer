# Contributing to Parquet Viewer

Bug reports, fixes, and improvements are welcome. Use the [issue tracker](https://github.com/rainism0329/parquet-viewer/issues) to report a problem or discuss a larger change before implementing it.

## Set up the project

1. Fork and clone [the repository](https://github.com/rainism0329/parquet-viewer).
2. Install JDK 21 and set `JAVA_HOME` to its installation directory.
3. Open the project as a Gradle project in IntelliJ IDEA, or use the included Gradle 8.13 Wrapper from a terminal. The first run requires network access to download dependencies and the IntelliJ development platform.

| Task | Windows PowerShell | macOS / Linux |
| --- | --- | --- |
| Run tests | `.\gradlew.bat test` | `sh ./gradlew test` |
| Build the installable ZIP | `.\gradlew.bat buildPlugin` | `sh ./gradlew buildPlugin` |
| Launch the IDE sandbox | `.\gradlew.bat runIde` | `sh ./gradlew runIde` |

The plugin ZIP is generated in `build/distributions/`. Production code is in `src/main/java`, plugin resources are in `src/main/resources`, and tests are in `src/Test/java` (explicitly registered as the Gradle test source directory).

## Report a problem

Include the plugin version, IntelliJ IDEA version, operating system, steps to reproduce, and the expected and actual behavior. A small synthetic Parquet file or schema is useful when a problem depends on the data. Remove personal, confidential, and proprietary data before attaching files or logs.

## Submit a change

- Keep each pull request focused and explain the user-visible change and the reason for it.
- Follow the style of the surrounding code. Add or update tests when changing behavior that can be tested meaningfully.
- Run the relevant tests and build the plugin. For UI changes, check the affected flow in the IDE sandbox and include a screenshot when it helps reviewers.
- State the checks you ran and any that you could not complete. Do not commit IDE caches, build output, credentials, or private datasets.

## Licensing contributions

By submitting a contribution for inclusion, you agree to license your original contribution under the project's [Apache License, Version 2.0](LICENSE). Only submit material you have the right to contribute, including any necessary permission from an employer or other rights holder.

If you add code, libraries, images, or other material from another source, identify its source and license, preserve required copyright and attribution notices, and update [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) as needed. Do not remove or relabel third-party licenses. Raise any uncertainty in the pull request so it can be resolved before merging.
