package com.bigphil.parquetviewer;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.page.PageReadStore;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ParquetViewerPanel {
    private final JPanel mainPanel;
    private final JPanel contentPanel;
    private final JTextArea schemaArea;
    private final JTable dataTable;
    private final JScrollPane dataScrollPane;
    private final JPanel checkboxPanel;
    private final JTextField columnSearchField;
    private final JTextField dataFilterField;
    private final JLabel fileLabel;
    private final JTabbedPane tabbedPane;
    private final JLabel pageInfoLabel;
    private final JLabel totalRowLabel;
    private final JCheckBox showAllRowsCheckbox;
    private final JButton exportCsvButton;
    private final JLabel filterCountLabel;

    private File currentFile;
    private MessageType currentSchema;
    private final List<String> allColumns = new ArrayList<>();

    private int currentPage = 1;
    private final int pageSize = 500;
    private int totalRowCount = 0;

    public ParquetViewerPanel() {
        mainPanel = new JPanel(new BorderLayout());

        // ===== Top: File Selection + File Name =====
        JButton chooseFileButton = new JButton("📁 Choose Parquet File");
        fileLabel = new JLabel("No file selected");
        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(chooseFileButton, BorderLayout.WEST);
        topPanel.add(fileLabel, BorderLayout.CENTER);
        mainPanel.add(topPanel, BorderLayout.NORTH);

        // ===== Left: Column Selection + Search =====
        columnSearchField = new JTextField();
        columnSearchField.setToolTipText("Search columns");
        columnSearchField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
        });

        checkboxPanel = new JPanel();
        checkboxPanel.setLayout(new BoxLayout(checkboxPanel, BoxLayout.Y_AXIS));
        JPanel leftPanel = new JPanel(new BorderLayout());
        leftPanel.add(columnSearchField, BorderLayout.NORTH);
        leftPanel.add(new JScrollPane(checkboxPanel), BorderLayout.CENTER);
        leftPanel.setPreferredSize(new Dimension(200, 500));
        mainPanel.add(leftPanel, BorderLayout.WEST);

        // ===== Center: Tabbed Panel (Schema / Data) =====
        schemaArea = new JTextArea();
        schemaArea.setEditable(false);
        schemaArea.setFont(new Font("Monospaced", Font.PLAIN, 12));

        dataTable = new JTable();
        dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        dataScrollPane = new JScrollPane(dataTable);

        dataFilterField = new JTextField();
        dataFilterField.setToolTipText("Filter by value (any column)");
        dataFilterField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { applyDataFilter(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { applyDataFilter(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { applyDataFilter(); }
        });

        JPanel filterBar = new JPanel(new BorderLayout(5, 5));
        filterBar.add(dataFilterField, BorderLayout.CENTER);

        JButton helpButton = new JButton("?");
        helpButton.setToolTipText("Click to view filter syntax help");
        helpButton.setMargin(new Insets(2, 6, 2, 6));
        helpButton.addActionListener(e -> showFilterHelpDialog());

        filterBar.add(helpButton, BorderLayout.EAST);

        JPanel dataTabPanel = new JPanel(new BorderLayout());
        dataTabPanel.add(dataScrollPane, BorderLayout.CENTER);
        dataTabPanel.add(filterBar, BorderLayout.NORTH);

        tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Schema", new JScrollPane(schemaArea));
        tabbedPane.addTab("Data", dataTabPanel);

        contentPanel = new JPanel(new BorderLayout());
        contentPanel.add(tabbedPane, BorderLayout.CENTER);
        mainPanel.add(contentPanel, BorderLayout.CENTER);

        // ===== Bottom: Pagination Controls + Show All Option + Export =====
        JPanel bottomPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        JButton prevButton = new JButton("<< Prev");
        JButton nextButton = new JButton("Next >>");
        pageInfoLabel = new JLabel("Page 0 of 0");
        showAllRowsCheckbox = new JCheckBox("Show all rows");
        exportCsvButton = new JButton("Export CSV");

        prevButton.addActionListener(e -> {
            if (currentPage > 1) {
                currentPage--;
                try {
                    showDataView(currentFile);
                } catch (IOException ex) {
                    showError("❌ Failed to update page: " + ex.getMessage());
                }
            }
        });

        nextButton.addActionListener(e -> {
            int totalPages = (int) Math.ceil((double) totalRowCount / pageSize);
            if (currentPage < totalPages) {
                currentPage++;
                try {
                    showDataView(currentFile);
                } catch (IOException ex) {
                    showError("❌ Failed to update page: " + ex.getMessage());
                }
            }
        });

        showAllRowsCheckbox.addActionListener(e -> {
            currentPage = 1;
            try {
                showDataView(currentFile);
            } catch (IOException ex) {
                showError("❌ Failed to update data: " + ex.getMessage());
            }
        });

        exportCsvButton.addActionListener(e -> exportCurrentTableToCSV());

        totalRowLabel = new JLabel("Total rows: 0");
        filterCountLabel = new JLabel("Showing 0 of 0 rows");

        bottomPanel.add(prevButton);
        bottomPanel.add(pageInfoLabel);
        bottomPanel.add(nextButton);
        bottomPanel.add(showAllRowsCheckbox);
        bottomPanel.add(exportCsvButton);
        bottomPanel.add(Box.createHorizontalStrut(20));
        bottomPanel.add(filterCountLabel);
        bottomPanel.add(totalRowLabel);

        mainPanel.add(bottomPanel, BorderLayout.SOUTH);

        // ===== File Load Button =====
        chooseFileButton.addActionListener(e -> {
            JFileChooser fileChooser = new JFileChooser();
            if (fileChooser.showOpenDialog(mainPanel) == JFileChooser.APPROVE_OPTION) {
                File file = fileChooser.getSelectedFile();

                if (!file.getName().toLowerCase().endsWith(".parquet")) {
                    JOptionPane.showMessageDialog(mainPanel,
                            "Please select a valid .parquet file.",
                            "Invalid File Type",
                            JOptionPane.WARNING_MESSAGE);
                    return;
                }

                try {
                    loadFile(file);
                } catch (IOException ex) {
                    showError("❌ Failed to read file: " + ex.getMessage());
                }
            }
        });

        columnSearchField.addActionListener(e -> {
            if (tabbedPane.getSelectedIndex() == 1 && currentFile!=null){
                currentPage =1;
                try {
                    showDataView(currentFile);
                } catch (IOException ex) {
                    showError("❌ Failed to read file: " + ex.getMessage());
                }
            }
        });
    }

    public JPanel getContent() {
        return mainPanel;
    }

    private void loadFile(File file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            currentFile = file;
            fileLabel.setText("File: " + file.getName());
            currentSchema = reader.getFooter().getFileMetaData().getSchema();
            allColumns.clear();
            currentSchema.getFields().forEach(f -> allColumns.add(f.getName()));
            updateCheckboxes();
            currentPage = 1;
            totalRowCount = estimateTotalRowCount(file);
            totalRowLabel.setText("Total rows: " + totalRowCount);
            showSchemaView();
            showDataView(file);
        }
    }

    private int estimateTotalRowCount(File file) throws IOException {
        int rowCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            PageReadStore pageStore;
            while ((pageStore = reader.readNextRowGroup()) != null) {
                rowCount += pageStore.getRowCount();
            }
        }
        return rowCount;
    }

    private void updateCheckboxes() {
        checkboxPanel.removeAll();
        String filter = columnSearchField.getText().trim().toLowerCase();

        for (String column : allColumns) {
            if (filter.isEmpty() || column.toLowerCase().contains(filter)) {
                JCheckBox checkBox = new JCheckBox(column);
                checkBox.setSelected(true);

                checkBox.addActionListener(e -> {
                    int selectedCount = getSelectedColumns().size();
                    if(!checkBox.isSelected()&& selectedCount<=0){
                        checkBox.setSelected(true);
                        JOptionPane.showMessageDialog(mainPanel, "Please select at least one column.", "Warning", JOptionPane.WARNING_MESSAGE);
                    }
                    if (tabbedPane.getSelectedIndex() == 1 && currentFile != null) {
                        currentPage = 1;
                        try {
                            showDataView(currentFile);
                        } catch (IOException ex) {
                            showError("❌ Failed to update data: " + ex.getMessage());
                        }
                    }
                });

                checkboxPanel.add(checkBox);
            }
        }

        checkboxPanel.revalidate();
        checkboxPanel.repaint();

        if (currentSchema != null ){
            schemaArea.setText(currentSchema.toString());
        }
    }

    private void showSchemaView() {
        schemaArea.setText(currentSchema.toString());
        tabbedPane.setSelectedIndex(0);
    }

    private void showDataView(File file) throws IOException {
        List<String> selectedColumns = getSelectedColumns();
        if (selectedColumns.isEmpty()) {
            showError("⚠ No columns selected.");
            tabbedPane.setSelectedIndex(0);
            return;
        }

        List<Object[]> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            PageReadStore pageStore;
            int skip = (currentPage - 1) * pageSize;
            int read = 0;
            int skipped = 0;

            while ((pageStore = reader.readNextRowGroup()) != null && (showAllRowsCheckbox.isSelected() || read < pageSize)) {
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(currentSchema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(currentSchema));
                long rowsInGroup = pageStore.getRowCount();

                for (int i = 0; i < rowsInGroup; i++) {
                    Group group = recordReader.read();
                    if (!showAllRowsCheckbox.isSelected() && skipped < skip) {
                        skipped++;
                        continue;
                    }
                    if (!showAllRowsCheckbox.isSelected() && read >= pageSize) break;
                    Object[] row = new Object[selectedColumns.size()];
                    for (int j = 0; j < selectedColumns.size(); j++) {
                        try {
                            int fieldIndex = currentSchema.getFieldIndex(selectedColumns.get(j));
                            row[j] = group.getValueToString(fieldIndex, 0);
                        } catch (Exception e) {
                            row[j] = "";
                        }
                    }
                    rows.add(row);
                    read++;
                }
            }
        }

        DefaultTableModel model = new DefaultTableModel(
                rows.toArray(new Object[0][]),
                selectedColumns.toArray()
        );
        dataTable.setModel(model);
        applyDataFilter();

        for (int i = 0; i < dataTable.getColumnCount(); i++) {
            TableColumn column = dataTable.getColumnModel().getColumn(i);
            column.setMinWidth(40);
            column.setPreferredWidth(100);
        }

        int totalPages = (int) Math.ceil((double) totalRowCount / pageSize);
        pageInfoLabel.setText(showAllRowsCheckbox.isSelected() ? "All rows shown" : "Page " + currentPage + " of " + totalPages);
        tabbedPane.setSelectedIndex(1);
    }

    private void applyDataFilter() {
        String filterText = dataFilterField.getText().trim();
        if (filterText.isEmpty()) {
            dataTable.setRowSorter(null);
            return;
        }

        String[] parts = filterText.split("(?i)\\s+AND\\s+");
        List<RowFilter<Object, Object>> filters = new ArrayList<>();

        for (String part : parts) {
            part = part.trim();
            if (part.contains("!=")) {
                String[] kv = part.split("!=");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    String val = kv[1].trim();
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(RowFilter.notFilter(RowFilter.regexFilter("(?i)^" + java.util.regex.Pattern.quote(val) + "$", getColumnIndex(col))));
                }
            } else if (part.contains(">=")) {
                String[] kv = part.split(">=");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    double val = Double.parseDouble(kv[1].trim());
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(new RowFilter<>() {
                        public boolean include(Entry<?, ?> entry) {
                            try {
                                return Double.parseDouble(entry.getValue(getColumnIndex(col)).toString()) >= val;
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    });
                }
            } else if (part.contains("<=")) {
                String[] kv = part.split("<=");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    double val = Double.parseDouble(kv[1].trim());
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(new RowFilter<>() {
                        public boolean include(Entry<?, ?> entry) {
                            try {
                                return Double.parseDouble(entry.getValue(getColumnIndex(col)).toString()) <= val;
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    });
                }
            } else if (part.contains(">")) {
                String[] kv = part.split(">");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    double val = Double.parseDouble(kv[1].trim());
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(new RowFilter<>() {
                        public boolean include(Entry<?, ?> entry) {
                            try {
                                return Double.parseDouble(entry.getValue(getColumnIndex(col)).toString()) > val;
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    });
                }
            } else if (part.contains("<")) {
                String[] kv = part.split("<");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    double val = Double.parseDouble(kv[1].trim());
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(new RowFilter<>() {
                        public boolean include(Entry<?, ?> entry) {
                            try {
                                return Double.parseDouble(entry.getValue(getColumnIndex(col)).toString()) < val;
                            } catch (Exception e) {
                                return false;
                            }
                        }
                    });
                }
            } else if (part.contains("~")) {
                String[] kv = part.split("~");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    String val = kv[1].trim();
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(RowFilter.regexFilter("(?i)" + java.util.regex.Pattern.quote(val), getColumnIndex(col)));
                }
            } else if (part.contains("=")) {
                String[] kv = part.split("=");
                if (kv.length == 2) {
                    String col = kv[0].trim();
                    String val = kv[1].trim();
                    int colIndex = getColumnIndex(col);
                    if (colIndex == -1) continue;
                    filters.add(RowFilter.regexFilter("(?i)^" + java.util.regex.Pattern.quote(val) + "$", getColumnIndex(col)));
                }
            }
        }

        RowFilter<Object, Object> combinedFilter = RowFilter.andFilter(filters);
        TableRowSorter<TableModel> sorter = new TableRowSorter<>(dataTable.getModel());
        sorter.setRowFilter(combinedFilter);
        dataTable.setRowSorter(sorter);

        int total = dataTable.getModel().getRowCount();
        int shown = dataTable.getRowCount();
        String format = String.format("Showing %,d of %,d rows", shown, total);
        filterCountLabel.setText(format);
    }

    private int getColumnIndex(String columnName) {
        TableModel model = dataTable.getModel();
        for (int i = 0; i < model.getColumnCount(); i++) {
            if (model.getColumnName(i).equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        return -1;
    }

    private List<String> getSelectedColumns() {
        List<String> selected = new ArrayList<>();
        for (Component comp : checkboxPanel.getComponents()) {
            if (comp instanceof JCheckBox) {
                JCheckBox cb = (JCheckBox) comp;
                if (cb.isSelected()) {
                    selected.add(cb.getText());
                }
            }
        }
        return selected;
    }

    private void exportCurrentTableToCSV() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Export CSV");

        if (currentFile != null) {
            String defaultName = currentFile.getName().replaceAll("\\.parquet$", "") + ".csv";
            fileChooser.setSelectedFile(new File(defaultName));
        }

        int userSelection = fileChooser.showSaveDialog(mainPanel);
        if (userSelection != JFileChooser.APPROVE_OPTION) return;

        File fileToSave = fileChooser.getSelectedFile();
        try (PrintWriter pw = new PrintWriter(new FileWriter(fileToSave))) {
            TableModel model = dataTable.getModel();
            TableRowSorter<?> sorter = (TableRowSorter<?>) dataTable.getRowSorter();

            for (int col = 0; col < model.getColumnCount(); col++) {
                pw.print(model.getColumnName(col));
                if (col < model.getColumnCount() - 1) pw.print(",");
            }
            pw.println();

            int rowCount = dataTable.getRowCount();
            for (int viewRow = 0; viewRow < rowCount; viewRow++) {
                int modelRow = dataTable.convertRowIndexToModel(viewRow);
                for (int col = 0; col < model.getColumnCount(); col++) {
                    Object val = model.getValueAt(modelRow, col);
                    pw.print(val != null ? val.toString().replaceAll(",", " ") : "");
                    if (col < model.getColumnCount() - 1) pw.print(",");
                }
                pw.println();
            }

            JOptionPane.showMessageDialog(mainPanel, "CSV exported to: " + fileToSave.getAbsolutePath());
        } catch (IOException e) {
            JOptionPane.showMessageDialog(mainPanel, "Failed to export CSV: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void showError(String message) {
        schemaArea.setText(message);
        tabbedPane.setSelectedIndex(0);
    }

    private void showFilterHelpDialog() {
        ImageIcon icon = new ImageIcon(getClass().getResource("/icons/donate3.png"));
        String helpText =
                "Supported filter syntax (case-insensitive):\n\n" +
                        "name=Alice             — exact match\n" +
                        "age!=30                — not equal\n" +
                        "score>80               — greater than\n" +
                        "score<=90              — less than or equal\n" +
                        "email~gmail            — contains substring\n" +
                        "country=US AND age<40  — combine multiple conditions\n\n" +
                        "Notes:\n" +
                        "• Use AND to combine multiple filters\n" +
                        "• Strings are matched ignoring case\n" +
                        "• Spaces around operators are allowed";

        JOptionPane.showMessageDialog(mainPanel, helpText, "Data Filter Help", JOptionPane.INFORMATION_MESSAGE, icon);
    }
}
