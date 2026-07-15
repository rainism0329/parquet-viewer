# Parquet Viewer for IntelliJ Platform

[![Version](https://img.shields.io/jetbrains/plugin/v/27306-parquet-viewer)](https://plugins.jetbrains.com/plugin/27306-parquet-viewer)
[![Downloads](https://img.shields.io/jetbrains/plugin/d/27306-parquet-viewer)](https://plugins.jetbrains.com/plugin/27306-parquet-viewer)

Explore local `.parquet` files inside an IntelliJ Platform tool window. Data stays on the local machine and no Spark, Hadoop service, notebook, or upload is required.

## Highlights

- Open several files in independent, closable tool-window tabs
- Read file metadata and data pages on cancellable background tasks
- Browse with regular pagination or bounded-memory virtualized **Show all** mode
- Search and choose visible columns without losing the current selection
- Filter loaded rows with SQL-like expressions and persistent history
- Sort numeric values by their real types
- Distinguish `NULL`, empty strings, loading cells, numbers, booleans, and text
- Copy cells, rectangular selections, rows as JSON, or inspect full/hex values
- Inspect searchable schema, row groups, compression, null counts, and column sizes
- Generate Hive DDL and Java POJOs from nested schemas
- Stream the current view or whole file to UTF-8 CSV/JSON

## Open a file

1. Open **Parquet Viewer** from the IDE tool-window bar.
2. Choose **Open Parquet File…**, or drop one or more `.parquet` files into the window.
3. Use the file tabs to switch between independent viewing sessions.

The viewer also remembers recently opened files for the current project.

## Row display modes

- **Paged** — loads a configurable number of rows at a time.
- **Show all · Smart** — materializes small results and automatically virtualizes large ones.
- **Show all · Memory** — explicitly loads all selected columns into IDE memory after a safety warning.
- **Show all · Virtual** — exposes continuous all-row scrolling while retaining only a bounded page cache.

## Filter examples

```text
name = 'Alice'
age >= 30 AND status != 'DISABLED'
country IN ('US', 'UK')
email IS NULL
name LIKE 'A%'
```

Filters currently apply to the rows loaded in the grid. Column names containing dots, hyphens, or Unicode characters are supported.

## Development

The project requires JDK 21. The repository includes Gradle Wrapper files:

```shell
./gradlew unitTest
./gradlew buildPlugin
```

## Author and support

Created by **Phil Zhang**.

- [Author website](https://phil-the-guy.zeabur.app/)
- [PayPal](https://www.paypal.com/paypalme/bigphilzhang)
- [Ko-fi](https://ko-fi.com/philipzhang51603)

## License

See the [End User License Agreement](https://phil-the-guy.zeabur.app/plugins-eula.html).
