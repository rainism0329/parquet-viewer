package com.bigphil.parquetviewer;

import com.intellij.ui.SearchTextField;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.ui.components.JBTabbedPane;
import com.intellij.ui.table.JBTable;
import com.intellij.util.ui.JBUI;

import javax.swing.JPanel;
import javax.swing.RowFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** File, row-group and column statistics presented as searchable native tables. */
public final class MetadataViewPanel {
    private final JPanel root = new JPanel(new BorderLayout());
    private final JBTabbedPane tabs = new JBTabbedPane();
    private final JBTable fileTable = createTable();
    private final JBTable rowGroupTable = createTable();
    private final JBTable columnTable = createTable();

    public MetadataViewPanel() {
        SearchTextField searchField = new SearchTextField(false);
        searchField.getTextEditor().getEmptyText().setText("Search metadata");
        searchField.addDocumentListener(new javax.swing.event.DocumentListener() {
            @Override public void insertUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
            @Override public void removeUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
            @Override public void changedUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
        });
        JPanel searchPanel = new JPanel(new BorderLayout());
        searchPanel.setBorder(JBUI.Borders.empty(5, 8));
        searchPanel.add(searchField, BorderLayout.CENTER);
        root.add(searchPanel, BorderLayout.NORTH);

        tabs.addTab("File", new JBScrollPane(fileTable));
        tabs.addTab("Row Groups", new JBScrollPane(rowGroupTable));
        tabs.addTab("Columns", new JBScrollPane(columnTable));
        tabs.addChangeListener(event -> filter(searchField.getText()));
        root.add(tabs, BorderLayout.CENTER);
    }

    public JPanel component() {
        return root;
    }

    public void setSummary(ParquetService.ParquetFileSummary summary) {
        List<Object[]> fileRows = new ArrayList<>();
        summary.extraMeta().forEach((key, value) -> fileRows.add(new Object[]{key, value}));
        fileTable.setModel(model(new String[]{"Property", "Value"}, fileRows));
        fileTable.getColumnModel().getColumn(0).setPreferredWidth(180);
        fileTable.getColumnModel().getColumn(1).setPreferredWidth(560);

        List<Object[]> rowGroups = new ArrayList<>();
        for (ParquetService.RowGroupDetails details : summary.rowGroups()) {
            rowGroups.add(new Object[]{
                    details.index(), details.rowCount(), details.totalByteSize(),
                    details.compressedSize(), details.startingPosition()
            });
        }
        rowGroupTable.setModel(model(
                new String[]{"Index", "Rows", "Uncompressed Bytes", "Compressed Bytes", "Start Offset"},
                rowGroups
        ));

        List<Object[]> columns = new ArrayList<>();
        for (Map.Entry<String, ParquetService.ColumnDetails> entry : summary.columnStats().entrySet()) {
            ParquetService.ColumnDetails details = entry.getValue();
            columns.add(new Object[]{
                    entry.getKey(), details.totalCount, details.totalNulls,
                    details.getNullPercentage(), details.totalCompressedSize,
                    details.totalUncompressedSize, details.getCompressionRatio()
            });
        }
        columnTable.setModel(model(
                new String[]{"Column Path", "Values", "Nulls", "Null %", "Compressed Bytes",
                        "Uncompressed Bytes", "Ratio"},
                columns
        ));

        installSorter(fileTable);
        installSorter(rowGroupTable);
        installSorter(columnTable);
    }

    private void filter(String query) {
        JBTable table = switch (tabs.getSelectedIndex()) {
            case 1 -> rowGroupTable;
            case 2 -> columnTable;
            default -> fileTable;
        };
        if (!(table.getRowSorter() instanceof TableRowSorter<?> rawSorter)) return;
        @SuppressWarnings("unchecked")
        TableRowSorter<TableModel> sorter = (TableRowSorter<TableModel>) rawSorter;
        String normalized = query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            sorter.setRowFilter(null);
        } else {
            sorter.setRowFilter(new RowFilter<TableModel, Integer>() {
                @Override
                public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                    for (int column = 0; column < entry.getValueCount(); column++) {
                        Object value = entry.getValue(column);
                        if (value != null && value.toString().toLowerCase(Locale.ROOT).contains(normalized)) return true;
                    }
                    return false;
                }
            });
        }
    }

    private static JBTable createTable() {
        JBTable table = new JBTable();
        table.setAutoResizeMode(JBTable.AUTO_RESIZE_OFF);
        table.setShowGrid(false);
        table.setStriped(true);
        table.setRowHeight(JBUI.scale(24));
        table.setDefaultRenderer(Object.class, new ParquetCellRenderer());
        return table;
    }

    private static DefaultTableModel model(String[] columns, List<Object[]> rows) {
        return new DefaultTableModel(rows.toArray(Object[][]::new), columns) {
            @Override public boolean isCellEditable(int row, int column) { return false; }
            @Override public Class<?> getColumnClass(int column) {
                for (Object[] row : rows) {
                    if (column < row.length && row[column] != null) return row[column].getClass();
                }
                return Object.class;
            }
        };
    }

    private static void installSorter(JBTable table) {
        table.setRowSorter(new TableRowSorter<>(table.getModel()));
    }
}
