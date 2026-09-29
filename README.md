# Parquet Viewer for IntelliJ IDEA


[![Version](https://img.shields.io/jetbrains/plugin/v/27306-parquet-viewer)](https://plugins.jetbrains.com/plugin/27306-parquet-viewer)
[![Downloads](https://img.shields.io/jetbrains/plugin/d/27306-parquet-viewer)](https://plugins.jetbrains.com/plugin/27306-parquet-viewer)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)



**Parquet Viewer** is a lightweight and intuitive IntelliJ IDEA plugin that allows you to open and explore `.parquet` files directly within the IDE — no need for external tools like Spark, Hadoop, or Jupyter.

[Source code](https://github.com/rainism0329/parquet-viewer) · [Report an issue](https://github.com/rainism0329/parquet-viewer/issues) · [Contributing](CONTRIBUTING.md)

---

## Features

- Open `.parquet` files from your local system
- View schema in a collapsible tree format
- Explore data in a tabular format with pagination
- Filter columns by name
- Select visible columns with a column selector
- Multi-row filtering by value (per-column filters)
- Auto-resize column widths
- Export the displayed data to CSV or JSON
- Inspect cell values in a hex/raw view
- Generate Hive DDL and Java POJOs from the schema

---

## Getting Started

1. Install the plugin from [JETBRAINS Marketplace](https://plugins.jetbrains.com/plugin/27306-parquet-viewer) 
2. Launch IntelliJ IDEA
3. Open the **Parquet Viewer Tool Window**
4. Click **"Choose Parquet File"** and select a `.parquet` file
5. Use the **Schema** and **Data** tabs to explore file contents

---

## Screenshots

![image](https://github.com/user-attachments/assets/ab3521f3-3753-4b97-956b-b0740f9b365e)
<img src="https://github.com/user-attachments/assets/0c00aaeb-c0bc-4038-8c4a-17fa52e88858" width="500"/>

---

## Build from source

Install **JDK 21** and set `JAVA_HOME` to its installation directory. The included Gradle Wrapper uses **Gradle 8.13**; a separate Gradle installation is not required. The first build downloads Gradle, dependencies, and the IntelliJ IDEA Community 2024.2.4 development platform, so it requires network access.

Clone [this repository](https://github.com/rainism0329/parquet-viewer), open a terminal in its root, and run:

```powershell
# Windows PowerShell
.\gradlew.bat buildPlugin
```

```sh
# macOS / Linux
sh ./gradlew buildPlugin
```

The installable plugin ZIP is written to `build/distributions/`. In IntelliJ IDEA, use **Settings → Plugins → ⚙ → Install Plugin from Disk** to install it.

To launch an IDE sandbox with the plugin, run `.\gradlew.bat runIde` on Windows or `sh ./gradlew runIde` on macOS/Linux. To run tests, use `.\gradlew.bat test` or `sh ./gradlew test` respectively. The `sh` form works even when the checkout does not preserve the executable bit. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

---

## Author

Created by **Phil Zhang**. 
Visit my personal website: [Home Page](https://phil-the-guy.zeabur.app/)

---

## Donate / 支持作者

If you find this plugin useful, consider supporting its development. Donations are entirely optional and are not required to use the plugin, access its source code, or contribute:
 
[**Donate via PayPal**](https://www.paypal.com/paypalme/bigphilzhang)

OR

[**Donate via Ko-fi**](https://ko-fi.com/philipzhang51603)

OR

**Alipay (支付宝打赏二维码):**

![Alipay QR](https://raw.githubusercontent.com/rainism0329/springclouddemo/master/1341746696680_.pic.jpg)

Thank you for your support!

---

## License

Parquet Viewer is open-source software licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE) for the full terms and [NOTICE](NOTICE) for the project attribution.

Third-party dependencies and assets retain their respective licenses and notices; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Maintainers preparing a release or a JetBrains open source support application can use the [release checklist](docs/OPEN_SOURCE_RELEASE_CHECKLIST.md).
