package com.bigphil.parquetviewer;

import com.intellij.openapi.ui.ComboBox;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetViewerPanel {
    private final JPanel mainPanel;
    private JPanel contentPanel;
    private JPanel wholeContentPanel;

    // Schema UI
    private JTextArea schemaTextArea;
    private JTree schemaTree;
    private JPanel schemaCardPanel;
    private CardLayout schemaCardLayout;

    // Metadata UI (New)
    private JTable metadataTable;

    // Data UI
    private JTable dataTable;
    private JScrollPane dataScrollPane;
    private JPanel checkboxPanel;
    private JTextField columnSearchField;
    private JTextField dataFilterField;

    // Common
    private JLabel fileLabel;
    private JTabbedPane tabbedPane;

    // Bottom Controls
    private JLabel pageInfoLabel;
    private JLabel totalRowLabel;
    private JCheckBox showAllRowsCheckbox;
    private JButton prevButton;
    private JButton nextButton;
    private JButton exportButton;
    private JLabel filterCountLabel;
    private JButton applyColumnFilterBtn;
    private JComboBox<String> exportFormatBox;

    private boolean hasMatchingColumns = true;
    private File currentFile;
    private MessageType currentSchema;
    private final List<String> allColumns = new ArrayList<>();

    private int currentPage = 1;
    private final int pageSize = 500;
    private long totalRowCount = 0;
    private Timer debounceTimer;

    public ParquetViewerPanel() {
        mainPanel = new JPanel();
        mainPanel.setLayout(new OverlayLayout(mainPanel));
        wholeContentPanel = new JPanel(new BorderLayout());
        mainPanel.add(wholeContentPanel);

        initTopPanel();
        initLeftPanel();
        initCenterPanel();
        initBottomPanel();

        setupListeners();
        setupDragDrop();

        updateControlState(false);
    }

    public JPanel getContent() { return mainPanel; }

    private void initTopPanel() {
        JButton chooseFileButton = new JButton("📁 Choose Parquet File");
        fileLabel = new JLabel("No file selected");
        JLabel tipLabel = new JLabel("Tip: You can also drag and drop a .parquet file to open it.");
        tipLabel.setForeground(Color.GRAY);
        tipLabel.setFont(tipLabel.getFont().deriveFont(Font.ITALIC, 11f));

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(chooseFileButton, BorderLayout.WEST);
        topPanel.add(fileLabel, BorderLayout.CENTER);
        topPanel.add(tipLabel, BorderLayout.EAST);
        wholeContentPanel.add(topPanel, BorderLayout.NORTH);

        chooseFileButton.addActionListener(e -> chooseFile());
    }

    private void initLeftPanel() {
        columnSearchField = new HintTextField("Type column name...");
        columnSearchField.setToolTipText("Search columns");
        columnSearchField.setTransferHandler(null);

        checkboxPanel = new JPanel();
        checkboxPanel.setLayout(new BoxLayout(checkboxPanel, BoxLayout.Y_AXIS));

        applyColumnFilterBtn = new JButton("✔");
        applyColumnFilterBtn.setToolTipText("Apply column filter and refresh data");
        applyColumnFilterBtn.setPreferredSize(new Dimension(24, 20));

        JPanel leftPanel = new JPanel(new BorderLayout());
        JPanel filterRow = new JPanel(new BorderLayout());
        filterRow.add(columnSearchField, BorderLayout.CENTER);
        filterRow.add(applyColumnFilterBtn, BorderLayout.EAST);

        leftPanel.add(filterRow, BorderLayout.NORTH);
        leftPanel.add(new JScrollPane(checkboxPanel), BorderLayout.CENTER);
        leftPanel.setPreferredSize(new Dimension(200, 500));
        wholeContentPanel.add(leftPanel, BorderLayout.WEST);
    }

    private void initCenterPanel() {
        // 1. Schema Tab
        schemaTextArea = new JTextArea();
        schemaTextArea.setEditable(false);
        schemaTextArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        schemaTextArea.setTransferHandler(null);

        schemaTree = new JTree(new DefaultMutableTreeNode("No Schema"));
        schemaTree.setTransferHandler(null);

        schemaCardLayout = new CardLayout();
        schemaCardPanel = new JPanel(schemaCardLayout);
        schemaCardPanel.add(new JScrollPane(schemaTextArea), "TEXT");
        schemaCardPanel.add(new JScrollPane(schemaTree), "TREE");

        JRadioButton textRadio = new JRadioButton("Text View", true);
        JRadioButton treeRadio = new JRadioButton("Tree View");
        ButtonGroup group = new ButtonGroup();
        group.add(textRadio); group.add(treeRadio);

        textRadio.addActionListener(e -> schemaCardLayout.show(schemaCardPanel, "TEXT"));
        treeRadio.addActionListener(e -> schemaCardLayout.show(schemaCardPanel, "TREE"));

        JPanel schemaToolbar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        schemaToolbar.add(new JLabel("Display Mode: "));
        schemaToolbar.add(textRadio); schemaToolbar.add(treeRadio);

        JPanel schemaTabContainer = new JPanel(new BorderLayout());
        schemaTabContainer.add(schemaToolbar, BorderLayout.NORTH);
        schemaTabContainer.add(schemaCardPanel, BorderLayout.CENTER);

        // 2. Metadata Tab (New)
        metadataTable = new JTable();
        metadataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        metadataTable.setEnabled(false); // Read-only
        JScrollPane metaScroll = new JScrollPane(metadataTable);

        // 3. Data Tab
        dataTable = new JTable();
        dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        dataScrollPane = new JScrollPane(dataTable);

        dataFilterField = new HintTextField("e.g. name='Alice' AND age<30 — press Enter to apply");
        dataFilterField.addActionListener(e -> applyDataFilter());
        dataFilterField.setTransferHandler(null);

        JButton helpButton = new JButton("?");
        helpButton.addActionListener(e -> FilterHelpDialog.show(mainPanel));

        JPanel filterBar = new JPanel(new BorderLayout(5, 5));
        filterBar.add(dataFilterField, BorderLayout.CENTER);
        filterBar.add(helpButton, BorderLayout.EAST);

        JPanel dataTabPanel = new JPanel(new BorderLayout());
        dataTabPanel.add(dataScrollPane, BorderLayout.CENTER);
        dataTabPanel.add(filterBar, BorderLayout.NORTH);

        // Tabbed Pane
        tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Schema", schemaTabContainer);
        tabbedPane.addTab("Metadata", metaScroll); // 新增 Metadata 页签
        tabbedPane.addTab("Data", dataTabPanel);

        contentPanel = new JPanel(new BorderLayout());
        contentPanel.add(tabbedPane, BorderLayout.CENTER);
        wholeContentPanel.add(contentPanel, BorderLayout.CENTER);
    }

    private void initBottomPanel() {
        prevButton = new JButton("<< Prev");
        nextButton = new JButton("Next >>");
        pageInfoLabel = new JLabel("Page 0 of 0");
        showAllRowsCheckbox = new JCheckBox("Show all rows");
        exportFormatBox = new ComboBox<>(new String[]{"CSV", "JSON"});
        exportButton = new JButton("Export CSV");
        filterCountLabel = new JLabel("Showing 0 of 0 rows");
        totalRowLabel = new JLabel("Total rows: 0");

        JPanel bottomPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 10, 5, 10);
        gbc.fill = GridBagConstraints.NONE;

        JPanel leftGroup = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        leftGroup.add(prevButton); leftGroup.add(pageInfoLabel);
        leftGroup.add(nextButton); leftGroup.add(showAllRowsCheckbox);
        gbc.gridx = 0; gbc.weightx = 0.3; gbc.anchor = GridBagConstraints.WEST;
        bottomPanel.add(leftGroup, gbc);

        JPanel centerGroup = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 0));
        centerGroup.add(new JLabel("Export type:"));
        centerGroup.add(exportFormatBox); centerGroup.add(exportButton);
        gbc.gridx = 1; gbc.weightx = 0.4; gbc.anchor = GridBagConstraints.CENTER;
        bottomPanel.add(centerGroup, gbc);

        JPanel rightGroup = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 0));
        rightGroup.add(filterCountLabel); rightGroup.add(totalRowLabel);
        gbc.gridx = 2; gbc.weightx = 0.3; gbc.anchor = GridBagConstraints.EAST;
        bottomPanel.add(rightGroup, gbc);

        wholeContentPanel.add(bottomPanel, BorderLayout.SOUTH);
    }

    private void setupListeners() {
        prevButton.addActionListener(e -> changePage(-1));
        nextButton.addActionListener(e -> changePage(1));
        showAllRowsCheckbox.addActionListener(e -> reloadDataView());

        exportFormatBox.addActionListener(e -> {
            String selected = (String) exportFormatBox.getSelectedItem();
            exportButton.setText("Export " + (selected == null ? "CSV" : selected.toUpperCase()));
        });

        exportButton.addActionListener(e -> {
            if ("JSON".equalsIgnoreCase((String) exportFormatBox.getSelectedItem())) {
                ParquetExporter.exportJson(mainPanel, dataTable, currentFile);
            } else {
                ParquetExporter.exportCsv(mainPanel, dataTable, currentFile);
            }
        });

        columnSearchField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { updateCheckboxes(); }
        });

        applyColumnFilterBtn.addActionListener(e -> reloadDataView());
        columnSearchField.addActionListener(e -> reloadDataView());

        // Tab Change Listener
        tabbedPane.addChangeListener(e -> {
            // Index 0=Schema, 1=Metadata, 2=Data
            boolean isDataTab = tabbedPane.getSelectedIndex() == 2;
            boolean hasFile = currentFile != null;
            updateControlState(hasFile && isDataTab);

            if (hasFile && isDataTab) {
                updatePaginationState();
            }
        });
    }

    private void setupDragDrop() {
        ParquetDropSupport.install(mainPanel, file -> {
            try { loadFile(file); } catch (IOException e) { showError("❌ Failed: " + e.getMessage()); }
        });
    }

    private void chooseFile() {
        JFileChooser fileChooser = new JFileChooser();
        if (fileChooser.showOpenDialog(mainPanel) == JFileChooser.APPROVE_OPTION) {
            File file = fileChooser.getSelectedFile();
            if (!file.getName().toLowerCase().endsWith(".parquet")) {
                JOptionPane.showMessageDialog(mainPanel, "Select a valid .parquet file.", "Warning", JOptionPane.WARNING_MESSAGE);
                return;
            }
            try { loadFile(file); } catch (IOException ex) { showError("❌ Failed: " + ex.getMessage()); }
        }
    }

    private void loadFile(File file) throws IOException {
        ParquetService.ParquetMetadata metadata = ParquetService.readMetadata(file);

        currentFile = file;
        fileLabel.setText("File: " + file.getName());
        currentSchema = metadata.schema();
        totalRowCount = metadata.rowCount();

        allColumns.clear();
        currentSchema.getFields().forEach(f -> allColumns.add(f.getName()));

        updateCheckboxes();
        currentPage = 1;
        totalRowLabel.setText("Total rows: " + String.format("%,d", totalRowCount));
        showAllRowsCheckbox.setSelected(false);

        // 1. Show Schema
        showSchemaView();

        // 2. Show Metadata (New)
        showMetadataView(metadata.extraMeta());

        // 3. Set Tab and Disable Controls
        tabbedPane.setSelectedIndex(0);
        updateControlState(false); // 此时在 Schema 页，立刻禁用按钮

        withLoadingDialog("Refreshing data...", () -> {
            try {
                showDataView(currentFile);
            } catch (IOException ex) {
                SwingUtilities.invokeLater(() -> showError("❌ Failed: " + ex.getMessage()));
            }
        });
    }

    private void showSchemaView() {
        schemaTextArea.setText(currentSchema.toString());
        schemaTextArea.setCaretPosition(0);

        DefaultMutableTreeNode root = new DefaultMutableTreeNode("Schema: " + currentSchema.getName());
        for (Type field : currentSchema.getFields()) {
            buildSchemaTree(root, field);
        }
        schemaTree.setModel(new DefaultTreeModel(root));
        for (int i = 0; i < schemaTree.getRowCount(); i++) schemaTree.expandRow(i);
    }

    // 新增：展示 Metadata
    private void showMetadataView(Map<String, String> meta) {
        String[] columns = {"Property", "Value"};
        Object[][] data = new Object[meta.size()][2];
        int i = 0;
        for (Map.Entry<String, String> entry : meta.entrySet()) {
            data[i][0] = entry.getKey();
            data[i][1] = entry.getValue();
            i++;
        }

        DefaultTableModel model = new DefaultTableModel(data, columns) {
            @Override
            public boolean isCellEditable(int row, int column) { return false; }
        };
        metadataTable.setModel(model);

        // 简单的列宽调整
        metadataTable.getColumnModel().getColumn(0).setPreferredWidth(150);
        metadataTable.getColumnModel().getColumn(1).setPreferredWidth(400);
    }

    private void buildSchemaTree(DefaultMutableTreeNode parent, Type type) {
        String nodeLabel;
        if (type.isPrimitive()) {
            String originalType = (type.getOriginalType() != null) ? " (" + type.getOriginalType() + ")" : "";
            nodeLabel = String.format("<html><b>%s</b> : <font color='blue'>%s</font>%s <font color='gray'>[%s]</font></html>",
                    type.getName(), type.asPrimitiveType().getPrimitiveTypeName(), originalType, type.getRepetition());
            parent.add(new DefaultMutableTreeNode(nodeLabel));
        } else {
            String originalType = (type.getOriginalType() != null) ? " (" + type.getOriginalType() + ")" : "";
            nodeLabel = String.format("<html><b>%s</b>%s <font color='gray'>[%s]</font></html>",
                    type.getName(), originalType, type.getRepetition());
            DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(nodeLabel);
            parent.add(groupNode);
            for (Type child : type.asGroupType().getFields()) buildSchemaTree(groupNode, child);
        }
    }

    private void showDataView(File file) throws IOException {
        List<String> selectedColumns = getSelectedColumns();
        if (selectedColumns.isEmpty()) return;

        List<Object[]> rows = ParquetService.readPageData(file, currentSchema, selectedColumns,
                currentPage, pageSize, showAllRowsCheckbox.isSelected());

        DefaultTableModel model = new DefaultTableModel(rows.toArray(new Object[0][]), selectedColumns.toArray());
        dataTable.setModel(model);
        applyDataFilter();

        for (int i = 0; i < dataTable.getColumnCount(); i++) {
            TableColumn column = dataTable.getColumnModel().getColumn(i);
            column.setMinWidth(40);
            column.setPreferredWidth(100);
        }

        updatePaginationState();
        updateFilterCountLabel();
    }

    private void updatePaginationState() {
        // --- 核心修复：只有在 Data Tab (index 2) 时才启用翻页逻辑 ---
        if (tabbedPane.getSelectedIndex() != 2) {
            prevButton.setEnabled(false);
            nextButton.setEnabled(false);
            return;
        }
        // --------------------------------------------------------

        int totalPages = (int) Math.ceil((double) totalRowCount / pageSize);
        if (showAllRowsCheckbox.isSelected()) {
            pageInfoLabel.setText("All rows shown");
            prevButton.setEnabled(false);
            nextButton.setEnabled(false);
        } else {
            pageInfoLabel.setText("Page " + currentPage + " of " + totalPages);
            prevButton.setEnabled(currentPage > 1);
            nextButton.setEnabled(currentPage < totalPages);
        }
    }

    private void changePage(int delta) {
        int totalPages = (int) Math.ceil((double) totalRowCount / pageSize);
        int newPage = currentPage + delta;
        if (newPage >= 1 && newPage <= totalPages) {
            currentPage = newPage;
            reloadDataView();
        }
    }

    private void reloadDataView() {
        if (currentFile != null) {
            if (debounceTimer != null && debounceTimer.isRunning()) debounceTimer.stop();
            debounceTimer = new Timer(200, e -> withLoadingDialog("Refreshing data...", () -> {
                try {
                    showDataView(currentFile);
                } catch (IOException ex) {
                    showError("❌ Failed: " + ex.getMessage());
                }
            }));
            debounceTimer.setRepeats(false);
            debounceTimer.start();
        }
    }

    private void applyDataFilter() {
        try {
            DataFilterParser parser = new DataFilterParser(dataTable.getModel());
            RowFilter<TableModel, Integer> filter = parser.parse(dataFilterField.getText().trim());
            TableRowSorter<TableModel> sorter = new TableRowSorter<>(dataTable.getModel());
            sorter.setRowFilter(filter);
            dataTable.setRowSorter(sorter);
            updateFilterCountLabel();
        } catch (Exception e) {}
    }

    private void updateCheckboxes() {
        checkboxPanel.removeAll();
        String filter = columnSearchField.getText().trim().toLowerCase();
        boolean anyMatch = false;
        for (String column : allColumns) {
            if (filter.isEmpty() || column.toLowerCase().contains(filter)) {
                JCheckBox checkBox = new JCheckBox(column);
                checkBox.setSelected(true);
                checkBox.addActionListener(e -> {
                    if (!checkBox.isSelected() && getSelectedColumns().isEmpty()) {
                        checkBox.setSelected(true);
                        JOptionPane.showMessageDialog(mainPanel, "Select at least one column.", "Warning", JOptionPane.WARNING_MESSAGE);
                    } else reloadDataView();
                });
                checkboxPanel.add(checkBox);
                anyMatch = true;
            }
        }
        if (!anyMatch) checkboxPanel.add(new JLabel("No matching columns."));
        hasMatchingColumns = anyMatch;
        applyColumnFilterBtn.setEnabled(anyMatch);
        checkboxPanel.revalidate(); checkboxPanel.repaint();
    }

    private List<String> getSelectedColumns() {
        List<String> selected = new ArrayList<>();
        for (Component comp : checkboxPanel.getComponents()) {
            if (comp instanceof JCheckBox cb && cb.isSelected()) selected.add(cb.getText());
        }
        return selected;
    }

    private void updateFilterCountLabel() {
        int total = dataTable.getModel().getRowCount();
        int shown = dataTable.getRowSorter() != null ? dataTable.getRowSorter().getViewRowCount() : total;
        filterCountLabel.setText(String.format("Showing %,d of %,d rows", shown, total));
    }

    private void updateControlState(boolean enabled) {
        prevButton.setEnabled(enabled);
        nextButton.setEnabled(enabled);
        exportFormatBox.setEnabled(enabled);
        exportButton.setEnabled(enabled);
        showAllRowsCheckbox.setEnabled(enabled);
        if (!enabled) {
            if (currentFile == null) exportButton.setToolTipText("Load a file to enable export");
            else exportButton.setToolTipText("Switch to Data tab to use controls");
        }
    }

    private void showError(String message) {
        schemaTextArea.setText(message);
        tabbedPane.setSelectedIndex(0);
        updateControlState(false);
    }

    private void withLoadingDialog(String message, Runnable task) {
        JDialog loadingDialog = new JDialog((Frame) null, "Loading", false);
        loadingDialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
        loadingDialog.setResizable(false);
        loadingDialog.setAlwaysOnTop(true);
        JPanel content = new JPanel(new BorderLayout(10, 10));
        content.setBorder(BorderFactory.createEmptyBorder(10, 20, 10, 20));
        ImageIcon icon = new ImageIcon(getClass().getResource("/icons/loading.gif"));
        if (icon.getImage() != null) content.add(new JLabel(new ImageIcon(icon.getImage().getScaledInstance(32, 32, Image.SCALE_DEFAULT))), BorderLayout.WEST);
        JLabel textLabel = new JLabel(message);
        textLabel.setFont(new Font("SansSerif", Font.PLAIN, 14));
        content.add(textLabel, BorderLayout.CENTER);
        loadingDialog.getContentPane().add(content);
        loadingDialog.pack();
        loadingDialog.setLocationRelativeTo(mainPanel);
        new Thread(() -> {
            try { SwingUtilities.invokeLater(() -> loadingDialog.setVisible(true)); task.run(); }
            catch (Exception ex) { SwingUtilities.invokeLater(() -> showError("❌ Failed: " + ex.getMessage())); }
            finally { SwingUtilities.invokeLater(loadingDialog::dispose); }
        }).start();
    }
}