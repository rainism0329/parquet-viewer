package com.bigphil.parquetviewer;

import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Mutable state for one file tab, intentionally independent from Swing components. */
public final class ParquetSessionState {
    public enum ShowAllStrategy {
        OFF,
        SMART,
        MATERIALIZED,
        VIRTUALIZED
    }

    private File file;
    private ParquetService.ParquetFileSummary summary;
    private final List<String> allColumns = new ArrayList<>();
    private final LinkedHashSet<String> visibleColumns = new LinkedHashSet<>();
    private final Map<String, Integer> columnWidths = new LinkedHashMap<>();
    private int currentPage = 1;
    private int pageSize = 500;
    private String filterText = "";
    private ShowAllStrategy showAllStrategy = ShowAllStrategy.OFF;

    public void initialize(File file, ParquetService.ParquetFileSummary summary, List<String> columns) {
        this.file = file;
        this.summary = summary;
        allColumns.clear();
        allColumns.addAll(columns);
        visibleColumns.clear();
        visibleColumns.addAll(columns);
        currentPage = 1;
        showAllStrategy = ShowAllStrategy.OFF;
    }

    public File file() { return file; }
    public ParquetService.ParquetFileSummary summary() { return summary; }
    public MessageType schema() { return summary == null ? null : summary.schema(); }
    public long rowCount() { return summary == null ? 0 : summary.rowCount(); }
    public List<String> allColumns() { return List.copyOf(allColumns); }
    public List<String> visibleColumns() { return List.copyOf(visibleColumns); }
    public Set<String> visibleColumnSet() { return Set.copyOf(visibleColumns); }
    public int currentPage() { return currentPage; }
    public void setCurrentPage(int currentPage) { this.currentPage = Math.max(1, currentPage); }
    public int pageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = Math.max(1, pageSize); }
    public String filterText() { return filterText; }
    public void setFilterText(String filterText) { this.filterText = filterText == null ? "" : filterText; }
    public ShowAllStrategy showAllStrategy() { return showAllStrategy; }
    public void setShowAllStrategy(ShowAllStrategy strategy) { this.showAllStrategy = strategy; }

    public void setVisibleColumns(List<String> columns) {
        visibleColumns.clear();
        for (String column : allColumns) {
            if (columns.contains(column)) visibleColumns.add(column);
        }
    }

    public void rememberColumnWidth(String column, int width) {
        if (column != null && width > 0) columnWidths.put(column, width);
    }

    public int preferredColumnWidth(String column, int fallback) {
        return columnWidths.getOrDefault(column, fallback);
    }
}
