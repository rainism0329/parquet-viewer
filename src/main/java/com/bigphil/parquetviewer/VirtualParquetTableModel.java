package com.bigphil.parquetviewer;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A bounded-memory table model for Show all rows. JTable can expose every logical
 * row while only nearby Parquet pages are retained in an LRU cache.
 */
public final class VirtualParquetTableModel extends AbstractTableModel {
    public enum LoadingValue { INSTANCE }

    public interface PageLoader {
        void loadPage(int page, PageCallback callback);
    }

    public interface PageCallback {
        void loaded(List<Object[]> rows);
        void failed(Throwable error);
    }

    private static final int DEFAULT_MAX_CACHED_PAGES = 12;

    private final MessageType schema;
    private final List<String> columns;
    private final int rowCount;
    private final int pageSize;
    private final PageLoader pageLoader;
    private final Set<Integer> pendingPages = ConcurrentHashMap.newKeySet();
    private final Map<Integer, List<Object[]>> pageCache;
    private volatile boolean disposed;

    public VirtualParquetTableModel(MessageType schema, List<String> columns, long totalRows,
                                    int pageSize, PageLoader pageLoader) {
        this.schema = schema;
        this.columns = List.copyOf(columns);
        this.rowCount = (int) Math.min(Integer.MAX_VALUE, Math.max(0, totalRows));
        this.pageSize = Math.max(1, pageSize);
        this.pageLoader = pageLoader;
        this.pageCache = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, List<Object[]>> eldest) {
                return size() > DEFAULT_MAX_CACHED_PAGES;
            }
        });
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) {
        return columns.get(column);
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        try {
            Type type = schema.getType(columns.get(columnIndex));
            if (type.isPrimitive() && !type.isRepetition(Type.Repetition.REPEATED)) {
                PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
                return switch (name) {
                    case INT32 -> Integer.class;
                    case INT64 -> Long.class;
                    case FLOAT -> Float.class;
                    case DOUBLE -> Double.class;
                    case BOOLEAN -> Boolean.class;
                    default -> Object.class;
                };
            }
        } catch (RuntimeException ignored) {
        }
        return Object.class;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (rowIndex < 0 || rowIndex >= rowCount) return null;
        int pageIndex = rowIndex / pageSize;
        List<Object[]> page = pageCache.get(pageIndex);
        if (page == null) {
            requestPage(pageIndex, true);
            return LoadingValue.INSTANCE;
        }

        int rowInPage = rowIndex % pageSize;
        if (rowInPage >= page.size()) return null;
        Object[] row = page.get(rowInPage);
        return columnIndex < row.length ? row[columnIndex] : null;
    }

    public int cachedPageCount() {
        return pageCache.size();
    }

    public void dispose() {
        disposed = true;
        pendingPages.clear();
        pageCache.clear();
    }

    private void requestPage(int pageIndex, boolean prefetchNextPage) {
        if (disposed || !pendingPages.add(pageIndex)) return;
        pageLoader.loadPage(pageIndex + 1, new PageCallback() {
            @Override
            public void loaded(List<Object[]> rows) {
                Runnable update = () -> {
                    pendingPages.remove(pageIndex);
                    if (disposed) return;
                    pageCache.put(pageIndex, List.copyOf(rows));
                    int firstRow = pageIndex * pageSize;
                    int lastRow = Math.min(rowCount - 1, firstRow + Math.max(0, rows.size() - 1));
                    if (lastRow >= firstRow) fireTableRowsUpdated(firstRow, lastRow);
                    if (prefetchNextPage && pageIndex + 1 < (rowCount + pageSize - 1) / pageSize) {
                        requestPage(pageIndex + 1, false);
                    }
                };
                if (SwingUtilities.isEventDispatchThread()) update.run();
                else SwingUtilities.invokeLater(update);
            }

            @Override
            public void failed(Throwable error) {
                pendingPages.remove(pageIndex);
            }
        });
    }
}
