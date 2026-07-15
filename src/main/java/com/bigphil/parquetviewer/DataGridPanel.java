package com.bigphil.parquetviewer;

import com.intellij.icons.AllIcons;
import com.intellij.openapi.ide.CopyPasteManager;
import com.intellij.openapi.ui.ComboBox;
import com.intellij.ui.SearchTextField;
import com.intellij.ui.components.JBLabel;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.ui.table.JBTable;
import com.intellij.util.ui.JBUI;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.KeyStroke;
import javax.swing.ListSelectionModel;
import javax.swing.RowFilter;
import javax.swing.SwingConstants;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The data-grid view and its immediate controls. File IO stays in the session controller. */
public final class DataGridPanel {
    private static final String FILTER_HINT =
            "Filter loaded rows, e.g. status='ERROR' AND duration>100";
    public enum RowDisplayMode {
        PAGED("Paged"),
        SHOW_ALL_SMART("Show all · Smart"),
        SHOW_ALL_MEMORY("Show all · Memory"),
        SHOW_ALL_VIRTUAL("Show all · Virtual");

        private final String label;
        RowDisplayMode(String label) { this.label = label; }
        @Override public String toString() { return label; }
    }

    public interface Listener {
        void previousPage();
        void nextPage();
        void pageSizeChanged(int pageSize);
        void rowDisplayModeChanged(RowDisplayMode mode);
        void chooseColumns();
        void exportData();
        void showFilterHelp();
    }

    private final Listener listener;
    private final JPanel root = new JPanel(new BorderLayout());
    private final JBTable table = new JBTable();
    private final JBScrollPane scrollPane = new JBScrollPane(table);
    private final SearchTextField filterField = new SearchTextField(false);
    private final JBLabel filterResultLabel = new JBLabel("No rows loaded");
    private final JBLabel pageLabel = new JBLabel("Page 0 of 0");
    private final JBLabel messageLabel = new JBLabel();
    private final JButton previousButton = new JButton(AllIcons.Actions.Back);
    private final JButton nextButton = new JButton(AllIcons.Actions.Forward);
    private final ComboBox<Integer> pageSizeBox = new ComboBox<>(new Integer[]{100, 500, 1000, 5000});
    private final ComboBox<RowDisplayMode> rowModeBox = new ComboBox<>(RowDisplayMode.values());
    private final Map<String, Integer> columnWidths = new HashMap<>();
    private int globalRowOffset;
    private long totalRows;
    private boolean virtualMode;
    private boolean suppressControlEvents;

    public DataGridPanel(Listener listener) {
        this.listener = listener;
        configureTable();
        root.add(createFilterBar(), BorderLayout.NORTH);
        root.add(scrollPane, BorderLayout.CENTER);
        root.add(createStatusBar(), BorderLayout.SOUTH);
        installContextMenu();
        installCopyShortcut();
    }

    public JPanel component() { return root; }
    public JBTable table() { return table; }
    public int selectedPageSize() { return (Integer) pageSizeBox.getSelectedItem(); }
    public RowDisplayMode rowDisplayMode() { return (RowDisplayMode) rowModeBox.getSelectedItem(); }
    public String filterText() { return filterField.getText(); }

    public void setLoading(String message) {
        messageLabel.setText(message);
        messageLabel.setIcon(AllIcons.Process.Step_1);
    }

    public void setMessage(String message) {
        messageLabel.setIcon(null);
        messageLabel.setText(message == null ? "" : message);
    }

    public void setError(String message) {
        messageLabel.setIcon(AllIcons.General.Error);
        messageLabel.setText(message == null ? "Operation failed" : message);
    }

    public void installMaterializedModel(TableModel model, int page, int pageSize,
                                         long totalRows, boolean allRows) {
        rememberColumnWidths();
        this.totalRows = totalRows;
        this.virtualMode = false;
        this.globalRowOffset = allRows ? 0 : Math.max(0, (page - 1) * pageSize);
        table.setRowSorter(null);
        table.setModel(model);
        installColumnWidths();
        filterField.setEnabled(true);
        filterField.getTextEditor().getEmptyText().setText(FILTER_HINT);
        applyFilter(false);
        updatePageState(page, pageSize, totalRows, allRows, false);
    }

    public void installVirtualModel(VirtualParquetTableModel model, long totalRows, int pageSize) {
        rememberColumnWidths();
        this.totalRows = totalRows;
        this.virtualMode = true;
        this.globalRowOffset = 0;
        table.setRowSorter(null);
        table.setModel(model);
        installColumnWidths();
        installRowHeader();
        filterField.setEnabled(false);
        filterField.getTextEditor().getEmptyText().setText("Filtering virtual all-rows mode is not loaded into memory");
        previousButton.setEnabled(false);
        nextButton.setEnabled(false);
        pageLabel.setText(String.format("All %,d rows · virtualized", totalRows));
        filterResultLabel.setText(String.format("%,d logical rows · %,d-row cache pages", totalRows, pageSize));
        if (model.getRowCount() > 0 && model.getColumnCount() > 0) model.getValueAt(0, 0);
    }

    public void setRowDisplayMode(RowDisplayMode mode) {
        suppressControlEvents = true;
        try {
            rowModeBox.setSelectedItem(mode);
        } finally {
            suppressControlEvents = false;
        }
    }

    public void disposeVirtualModel() {
        if (table.getModel() instanceof VirtualParquetTableModel virtualModel) virtualModel.dispose();
    }

    public void reapplyFilter() {
        applyFilter(false);
    }

    private void configureTable() {
        table.setAutoResizeMode(JBTable.AUTO_RESIZE_OFF);
        table.setCellSelectionEnabled(true);
        table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        table.setShowGrid(false);
        table.setStriped(true);
        table.setRowHeight(JBUI.scale(24));
        ParquetCellRenderer renderer = new ParquetCellRenderer();
        table.setDefaultRenderer(Object.class, renderer);
        table.setDefaultRenderer(Integer.class, renderer);
        table.setDefaultRenderer(Long.class, renderer);
        table.setDefaultRenderer(Float.class, renderer);
        table.setDefaultRenderer(Double.class, renderer);
        table.setDefaultRenderer(Boolean.class, renderer);
    }

    private JPanel createFilterBar() {
        JPanel bar = new JPanel(new BorderLayout(JBUI.scale(6), 0));
        bar.setBorder(JBUI.Borders.empty(5, 8));
        filterField.getTextEditor().getEmptyText().setText(FILTER_HINT);
        filterField.getTextEditor().addActionListener(event -> applyFilter(true));
        bar.add(filterField, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, JBUI.scale(4), 0));
        JButton history = new JButton(AllIcons.Actions.SearchWithHistory);
        history.setToolTipText("Filter history");
        history.addActionListener(event -> showHistory(history));
        JButton help = new JButton(AllIcons.Actions.Help);
        help.setToolTipText("Filter syntax help");
        help.addActionListener(event -> listener.showFilterHelp());
        actions.add(history);
        actions.add(help);
        bar.add(actions, BorderLayout.EAST);
        return bar;
    }

    private JPanel createStatusBar() {
        JPanel bar = new JPanel(new BorderLayout(JBUI.scale(8), 0));
        bar.setBorder(JBUI.Borders.empty(5, 8));

        JPanel left = new JPanel(new FlowLayout(FlowLayout.LEFT, JBUI.scale(5), 0));
        previousButton.setToolTipText("Previous page");
        nextButton.setToolTipText("Next page");
        previousButton.addActionListener(event -> listener.previousPage());
        nextButton.addActionListener(event -> listener.nextPage());
        pageSizeBox.setSelectedItem(500);
        pageSizeBox.setToolTipText("Rows per page or virtual cache page");
        pageSizeBox.addActionListener(event -> listener.pageSizeChanged(selectedPageSize()));
        rowModeBox.addActionListener(event -> {
            if (!suppressControlEvents) listener.rowDisplayModeChanged(rowDisplayMode());
        });
        JButton columns = new JButton("Columns…", AllIcons.Nodes.DataTables);
        columns.addActionListener(event -> listener.chooseColumns());
        JButton export = new JButton("Export…", AllIcons.ToolbarDecorator.Export);
        export.addActionListener(event -> listener.exportData());
        left.add(previousButton);
        left.add(pageLabel);
        left.add(nextButton);
        left.add(pageSizeBox);
        left.add(rowModeBox);
        left.add(columns);
        left.add(export);
        bar.add(left, BorderLayout.WEST);

        JPanel right = new JPanel(new FlowLayout(FlowLayout.RIGHT, JBUI.scale(12), 0));
        right.add(messageLabel);
        right.add(filterResultLabel);
        bar.add(right, BorderLayout.EAST);
        return bar;
    }

    private void applyFilter(boolean addToHistory) {
        if (virtualMode) return;
        String expression = filterField.getText().trim();
        try {
            TableRowSorter<TableModel> sorter = new TableRowSorter<>(table.getModel());
            RowFilter<TableModel, Integer> filter = new DataFilterParser(table.getModel()).parse(expression);
            sorter.setRowFilter(filter);
            table.setRowSorter(sorter);
            installRowHeader();
            if (addToHistory && !expression.isEmpty()) FilterHistoryManager.add(expression);
            updateFilterCount();
            setMessage(expression.isEmpty() ? "" : "Filter applied to loaded rows");
        } catch (IllegalArgumentException error) {
            setError(error.getMessage());
        }
    }

    private void updateFilterCount() {
        int modelRows = table.getModel().getRowCount();
        int visibleRows = table.getRowSorter() == null ? modelRows : table.getRowSorter().getViewRowCount();
        filterResultLabel.setText(String.format("Showing %,d of %,d loaded rows", visibleRows, modelRows));
    }

    private void updatePageState(int page, int pageSize, long totalRows, boolean allRows, boolean virtual) {
        if (allRows) {
            previousButton.setEnabled(false);
            nextButton.setEnabled(false);
            pageLabel.setText(String.format("All %,d rows%s", totalRows, virtual ? " · virtualized" : ""));
        } else {
            long totalPages = Math.max(1, (totalRows + pageSize - 1) / pageSize);
            pageLabel.setText(String.format("Page %,d of %,d", page, totalPages));
            previousButton.setEnabled(page > 1);
            nextButton.setEnabled(page < totalPages);
        }
        updateFilterCount();
    }

    private void rememberColumnWidths() {
        for (int index = 0; index < table.getColumnCount(); index++) {
            TableColumn column = table.getColumnModel().getColumn(index);
            columnWidths.put(String.valueOf(column.getHeaderValue()), column.getWidth());
        }
    }

    private void installColumnWidths() {
        for (int index = 0; index < table.getColumnCount(); index++) {
            TableColumn column = table.getColumnModel().getColumn(index);
            column.setMinWidth(JBUI.scale(65));
            column.setPreferredWidth(columnWidths.getOrDefault(String.valueOf(column.getHeaderValue()), JBUI.scale(140)));
        }
    }

    private void installRowHeader() {
        RowNumberTable rowNumbers = new RowNumberTable();
        scrollPane.setRowHeaderView(rowNumbers);
        scrollPane.setCorner(JScrollPane.UPPER_LEFT_CORNER, rowNumbers.getTableHeader());
    }

    private void showHistory(Component invoker) {
        JPopupMenu popup = new JPopupMenu();
        List<String> history = FilterHistoryManager.getAll();
        if (history.isEmpty()) {
            JMenuItem empty = new JMenuItem("No filter history");
            empty.setEnabled(false);
            popup.add(empty);
        } else {
            for (String expression : history) {
                JMenuItem item = new JMenuItem(expression);
                item.addActionListener(event -> {
                    filterField.setText(expression);
                    applyFilter(false);
                });
                popup.add(item);
            }
            popup.addSeparator();
            JMenuItem clear = new JMenuItem("Clear history");
            clear.addActionListener(event -> FilterHistoryManager.clear());
            popup.add(clear);
        }
        popup.show(invoker, 0, invoker.getHeight());
    }

    private void installContextMenu() {
        table.addMouseListener(new MouseAdapter() {
            @Override public void mousePressed(MouseEvent event) { showMenu(event); }
            @Override public void mouseReleased(MouseEvent event) { showMenu(event); }

            private void showMenu(MouseEvent event) {
                if (!event.isPopupTrigger()) return;
                int row = table.rowAtPoint(event.getPoint());
                int column = table.columnAtPoint(event.getPoint());
                if (row < 0 || column < 0) return;
                if (!table.isCellSelected(row, column)) table.changeSelection(row, column, false, false);

                JPopupMenu menu = new JPopupMenu();
                JMenuItem copy = new JMenuItem("Copy Selection");
                copy.addActionListener(action -> copySelection());
                JMenuItem copyJson = new JMenuItem("Copy Row as JSON");
                copyJson.addActionListener(action -> copyRowAsJson(row));
                JMenuItem inspect = new JMenuItem("Inspect Full Value / Hex…");
                inspect.addActionListener(action -> new ValueInspectorDialog(
                        table.getColumnName(column), table.getValueAt(row, column)
                ).show());
                JMenuItem filterValue = new JMenuItem("Filter by This Value");
                filterValue.setEnabled(!virtualMode);
                filterValue.addActionListener(action -> filterByValue(row, column, false));
                JMenuItem excludeValue = new JMenuItem("Exclude This Value");
                excludeValue.setEnabled(!virtualMode);
                excludeValue.addActionListener(action -> filterByValue(row, column, true));
                menu.add(copy);
                menu.add(copyJson);
                menu.add(inspect);
                menu.addSeparator();
                menu.add(filterValue);
                menu.add(excludeValue);
                menu.show(table, event.getX(), event.getY());
            }
        });
    }

    private void installCopyShortcut() {
        int shortcutMask = Toolkit.getDefaultToolkit().getMenuShortcutKeyMaskEx();
        table.getInputMap(JComponent.WHEN_FOCUSED).put(KeyStroke.getKeyStroke('C', shortcutMask), "copy-selection");
        table.getActionMap().put("copy-selection", new AbstractAction() {
            @Override public void actionPerformed(ActionEvent event) { copySelection(); }
        });
    }

    private void copySelection() {
        int[] rows = table.getSelectedRows();
        int[] columns = table.getSelectedColumns();
        if (rows.length == 0 || columns.length == 0) return;
        StringBuilder value = new StringBuilder();
        for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            if (rowIndex > 0) value.append('\n');
            for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
                if (columnIndex > 0) value.append('\t');
                Object cell = table.getValueAt(rows[rowIndex], columns[columnIndex]);
                if (cell != null && cell != VirtualParquetTableModel.LoadingValue.INSTANCE) {
                    value.append(String.valueOf(cell));
                }
            }
        }
        CopyPasteManager.getInstance().setContents(new StringSelection(value.toString()));
        setMessage(String.format("Copied %,d × %,d cells", rows.length, columns.length));
    }

    private void copyRowAsJson(int viewRow) {
        int modelRow = table.convertRowIndexToModel(viewRow);
        StringBuilder json = new StringBuilder("{");
        for (int column = 0; column < table.getModel().getColumnCount(); column++) {
            if (column > 0) json.append(',');
            json.append('\n').append("  \"").append(jsonEscape(table.getModel().getColumnName(column))).append("\": ");
            Object value = table.getModel().getValueAt(modelRow, column);
            if (value == VirtualParquetTableModel.LoadingValue.INSTANCE) {
                setError("This virtual row is still loading");
                return;
            }
            if (value == null) json.append("null");
            else if (isNonFiniteNumber(value)) {
                json.append('"').append(value).append('"');
            }
            else if (value instanceof Number || value instanceof Boolean) json.append(value);
            else json.append('"').append(jsonEscape(String.valueOf(value))).append('"');
        }
        json.append("\n}");
        CopyPasteManager.getInstance().setContents(new StringSelection(json.toString()));
        setMessage("Row copied as JSON");
    }

    private void filterByValue(int viewRow, int viewColumn, boolean exclude) {
        Object value = table.getValueAt(viewRow, viewColumn);
        String column = table.getColumnName(viewColumn);
        String expression;
        if (value == null) expression = column + (exclude ? " IS NOT NULL" : " IS NULL");
        else if (value instanceof Number || value instanceof Boolean) expression = column + (exclude ? "!=" : "=") + value;
        else expression = column + (exclude ? "!='" : "='") + String.valueOf(value).replace("'", "\\'") + "'";
        filterField.setText(expression);
        applyFilter(true);
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

    private final class RowNumberTable extends JBTable {
        private RowNumberTable() {
            super(new AbstractTableModel() {
                @Override public int getRowCount() { return table.getRowCount(); }
                @Override public int getColumnCount() { return 1; }
                @Override public Object getValueAt(int rowIndex, int columnIndex) {
                    int modelRow = table.getRowSorter() == null ? rowIndex : table.convertRowIndexToModel(rowIndex);
                    return (long) globalRowOffset + modelRow + 1;
                }
                @Override public String getColumnName(int column) { return "#"; }
            });
            setFocusable(false);
            setRowHeight(table.getRowHeight());
            setPreferredScrollableViewportSize(new Dimension(JBUI.scale(58), 0));
            setSelectionModel(table.getSelectionModel());
            setShowGrid(false);
            getColumnModel().getColumn(0).setCellRenderer(new DefaultTableCellRenderer() {
                {
                    setHorizontalAlignment(SwingConstants.RIGHT);
                    setBorder(JBUI.Borders.emptyRight(6));
                }
            });
        }
    }
}
