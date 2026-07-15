package com.bigphil.parquetviewer;

import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns cancellable foreground IO for a single Parquet tab. UI classes never read
 * Parquet data directly and stale task results are discarded by generation id.
 */
public final class ParquetSessionController {
    public interface Listener {
        void onLoading(String message);
        void onFileLoaded(FileLoadResult result);
        void onPageLoaded(PageLoadResult result);
        void onFailure(String operation, Throwable error);
        void onCancelled();
    }

    public record FileLoadResult(File file, ParquetService.ParquetFileSummary summary,
                                 List<String> columns, List<Object[]> firstPage,
                                 int pageSize) {}

    public record PageLoadResult(File file, MessageType schema, List<String> columns,
                                 List<Object[]> rows, int page, int pageSize,
                                 boolean allRowsMaterialized) {}

    private final Project project;
    private final Listener listener;
    private final AtomicLong generation = new AtomicLong();
    private final AtomicReference<ProgressIndicator> activeIndicator = new AtomicReference<>();
    private volatile boolean disposed;

    public ParquetSessionController(Project project, Listener listener) {
        this.project = project;
        this.listener = listener;
    }

    public void openFile(File file, int pageSize) {
        long request = beginRequest("Loading " + file.getName() + "…");

        new Task.Backgroundable(project, "Open Parquet File", true) {
            private FileLoadResult result;

            @Override
            public void run(@NotNull ProgressIndicator indicator) {
                try {
                    if (!activate(request, indicator)) return;
                    indicator.setIndeterminate(true);
                    indicator.setText("Reading file metadata");
                    ParquetService.ParquetFileSummary summary = ParquetService.readFileSummary(file, indicator);

                    List<String> columns = new ArrayList<>();
                    summary.schema().getFields().forEach(field -> columns.add(field.getName()));
                    indicator.setText("Loading the first " + pageSize + " rows");
                    List<Object[]> rows = ParquetService.readPageData(
                            file, summary.schema(), columns, 1, pageSize, false, indicator
                    );
                    result = new FileLoadResult(file, summary, List.copyOf(columns), rows, pageSize);
                } catch (java.io.IOException error) {
                    throw new RuntimeException(error);
                }
            }

            @Override
            public void onSuccess() {
                if (isCurrent(request) && result != null) listener.onFileLoaded(result);
                finish(request);
            }

            @Override
            public void onThrowable(@NotNull Throwable error) {
                if (isCurrent(request)) listener.onFailure("Open file", error);
                finish(request);
            }

            @Override
            public void onCancel() {
                if (isCurrent(request)) listener.onCancelled();
                finish(request);
            }
        }.queue();
    }

    public void loadPage(File file, MessageType schema, List<String> columns,
                         int page, int pageSize, boolean materializeAllRows) {
        long request = beginRequest(materializeAllRows ? "Loading all rows…" : "Loading page " + page + "…");
        List<String> requestedColumns = List.copyOf(columns);

        new Task.Backgroundable(project, materializeAllRows ? "Load All Parquet Rows" : "Load Parquet Page", true) {
            private PageLoadResult result;

            @Override
            public void run(@NotNull ProgressIndicator indicator) {
                try {
                    if (!activate(request, indicator)) return;
                    indicator.setIndeterminate(true);
                    indicator.setText(materializeAllRows ? "Reading all rows" : "Reading page " + page);
                    List<Object[]> rows = ParquetService.readPageData(
                            file, schema, requestedColumns, page, pageSize, materializeAllRows, indicator
                    );
                    result = new PageLoadResult(
                            file, schema, requestedColumns, rows, page, pageSize, materializeAllRows
                    );
                } catch (java.io.IOException error) {
                    throw new RuntimeException(error);
                }
            }

            @Override
            public void onSuccess() {
                if (isCurrent(request) && result != null) listener.onPageLoaded(result);
                finish(request);
            }

            @Override
            public void onThrowable(@NotNull Throwable error) {
                if (isCurrent(request)) listener.onFailure("Load data", error);
                finish(request);
            }

            @Override
            public void onCancel() {
                if (isCurrent(request)) listener.onCancelled();
                finish(request);
            }
        }.queue();
    }

    public void cancelCurrent() {
        generation.incrementAndGet();
        ProgressIndicator indicator = activeIndicator.getAndSet(null);
        if (indicator != null) indicator.cancel();
    }

    public void dispose() {
        disposed = true;
        cancelCurrent();
    }

    private long beginRequest(String message) {
        cancelCurrent();
        long request = generation.incrementAndGet();
        listener.onLoading(message);
        return request;
    }

    private boolean activate(long request, ProgressIndicator indicator) {
        if (!isCurrent(request)) {
            indicator.cancel();
            return false;
        }
        activeIndicator.set(indicator);
        return true;
    }

    private boolean isCurrent(long request) {
        return !disposed && generation.get() == request;
    }

    private void finish(long request) {
        if (generation.get() == request) activeIndicator.set(null);
    }
}
