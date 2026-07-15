package com.bigphil.parquetviewer;

import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.Consumer;

/** Cancellable UTF-8 streaming export with atomic destination replacement. */
public final class ParquetExportService {
    public record Request(File sourceFile, MessageType schema, List<String> columns,
                          long totalRows, List<Object[]> currentRows,
                          ExportOptionsDialog.Scope scope,
                          ExportOptionsDialog.Format format, File destination) {}

    private static final int EXPORT_PAGE_SIZE = 5000;

    private ParquetExportService() {}

    public static void export(Project project, Request request,
                              Consumer<String> success, Consumer<String> failure) {
        new Task.Backgroundable(project, "Export Parquet Data", true) {
            private long exportedRows;

            @Override
            public void run(@NotNull ProgressIndicator indicator) {
                indicator.setIndeterminate(false);
                indicator.setText("Preparing export");
                Path destination = request.destination().toPath();
                Path parent = destination.toAbsolutePath().getParent();
                Path temporary = null;
                try {
                    if (parent != null) Files.createDirectories(parent);
                    temporary = Files.createTempFile(parent, ".parquet-viewer-", ".tmp");
                    try (BufferedWriter writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
                        if (request.format() == ExportOptionsDialog.Format.CSV) writeCsv(writer, request, indicator);
                        else writeJson(writer, request, indicator);
                    }
                    indicator.checkCanceled();
                    moveIntoPlace(temporary, destination);
                    temporary = null;
                } catch (IOException error) {
                    throw new RuntimeException(error);
                } finally {
                    if (temporary != null) {
                        try { Files.deleteIfExists(temporary); } catch (IOException ignored) {}
                    }
                }
            }

            @Override
            public void onSuccess() {
                success.accept(String.format("Exported %,d rows to %s", exportedRows, request.destination().getName()));
            }

            @Override
            public void onThrowable(@NotNull Throwable error) {
                Throwable cause = error.getCause() == null ? error : error.getCause();
                failure.accept(cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage());
            }

            @Override
            public void onCancel() {
                success.accept("Export cancelled");
            }

            private void writeCsv(BufferedWriter writer, Request request, ProgressIndicator indicator) throws IOException {
                writeCsvRow(writer, request.columns().toArray());
                forEachRow(request, indicator, row -> {
                    writeCsvRow(writer, row);
                    exportedRows++;
                });
            }

            private void writeJson(BufferedWriter writer, Request request, ProgressIndicator indicator) throws IOException {
                writer.write("[\n");
                final boolean[] first = {true};
                forEachRow(request, indicator, row -> {
                    if (!first[0]) writer.write(",\n");
                    first[0] = false;
                    writer.write("  {");
                    for (int column = 0; column < request.columns().size(); column++) {
                        if (column > 0) writer.write(", ");
                        writer.write('"' + jsonEscape(request.columns().get(column)) + "\":");
                        writeJsonValue(writer, column < row.length ? row[column] : null);
                    }
                    writer.write("}");
                    exportedRows++;
                });
                writer.write("\n]\n");
            }

            private void forEachRow(Request request, ProgressIndicator indicator,
                                    ThrowingRowConsumer consumer) throws IOException {
                if (request.scope() == ExportOptionsDialog.Scope.CURRENT_VIEW) {
                    List<Object[]> rows = request.currentRows();
                    for (int index = 0; index < rows.size(); index++) {
                        if ((index & 0xFF) == 0) indicator.checkCanceled();
                        consumer.accept(rows.get(index));
                        indicator.setFraction(rows.isEmpty() ? 1 : (double) (index + 1) / rows.size());
                    }
                    return;
                }

                long pages = Math.max(1, (request.totalRows() + EXPORT_PAGE_SIZE - 1) / EXPORT_PAGE_SIZE);
                for (int page = 1; page <= pages; page++) {
                    indicator.checkCanceled();
                    indicator.setText("Exporting page " + page + " of " + pages);
                    List<Object[]> rows = ParquetService.readPageData(
                            request.sourceFile(), request.schema(), request.columns(),
                            page, EXPORT_PAGE_SIZE, false, indicator
                    );
                    for (Object[] row : rows) consumer.accept(row);
                    indicator.setFraction((double) page / pages);
                }
            }
        }.queue();
    }

    private static void writeCsvRow(BufferedWriter writer, Object[] row) throws IOException {
        for (int column = 0; column < row.length; column++) {
            if (column > 0) writer.write(',');
            writer.write(csvEscape(row[column]));
        }
        writer.newLine();
    }

    private static String csvEscape(Object value) {
        if (value == null) return "";
        String text = String.valueOf(value);
        if (text.indexOf(',') >= 0 || text.indexOf('"') >= 0 || text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
            return '"' + text.replace("\"", "\"\"") + '"';
        }
        return text;
    }

    private static void writeJsonValue(BufferedWriter writer, Object value) throws IOException {
        if (value == null) writer.write("null");
        else if (isNonFiniteNumber(value)) writer.write('"' + String.valueOf(value) + '"');
        else if (value instanceof Number || value instanceof Boolean) writer.write(String.valueOf(value));
        else writer.write('"' + jsonEscape(String.valueOf(value)) + '"');
    }

    private static String jsonEscape(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) escaped.append(String.format("\\u%04x", (int) character));
                    else escaped.append(character);
                }
            }
        }
        return escaped.toString();
    }

    private static boolean isNonFiniteNumber(Object value) {
        return value instanceof Double doubleValue && !Double.isFinite(doubleValue)
                || value instanceof Float floatValue && !Float.isFinite(floatValue);
    }

    private static void moveIntoPlace(Path temporary, Path destination) throws IOException {
        try {
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @FunctionalInterface
    private interface ThrowingRowConsumer {
        void accept(Object[] row) throws IOException;
    }
}
