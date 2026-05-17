package com.bigphil.parquetviewer;

import com.intellij.icons.AllIcons;
import com.intellij.openapi.ui.ComboBox;
import com.intellij.ui.JBColor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.swing.*;
import javax.swing.table.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    // Metadata UI
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
    private ComboBox<String> exportFormatBox;

    private boolean hasMatchingColumns = true;
    private File currentFile;
    private MessageType currentSchema;
    private final List<String> allColumns = new ArrayList<>();

    private int currentPage = 1;
    private final int pageSize = 500;
    private long totalRowCount = 0;
    private Timer debounceTimer;
    private Map<String, ParquetService.ColumnDetails> currentColumnStats;

    // --- Easter Egg State ---
    private boolean isRGBMode = false;
    private boolean isCRTMode = false;
    private Timer rgbTimer;
    private float hueOffset = 0.0f;

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
        setupRGBTimer();

        updateControlState(false);
    }

    public JPanel getContent() { return mainPanel; }

    private void setupRGBTimer() {
        rgbTimer = new Timer(33, e -> {
            if (isRGBMode && dataTable.isShowing()) {
                hueOffset += 0.005f;
                if (hueOffset > 1.0f) hueOffset = 0.0f;
                dataTable.repaint();
                // 同步刷新表头
                if (dataTable.getTableHeader() != null) {
                    dataTable.getTableHeader().repaint();
                }
                // ✨ 刷新元数据表
                if (metadataTable.isShowing()) {
                    metadataTable.repaint();
                    if (metadataTable.getTableHeader() != null) metadataTable.getTableHeader().repaint();
                }
            }
        });
    }

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
        schemaTextArea = new JTextArea();
        schemaTextArea.setEditable(false);
        schemaTextArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        schemaTextArea.setTransferHandler(null);

        schemaTree = new JTree(new DefaultMutableTreeNode("No Schema"));
        schemaTree.setTransferHandler(null);
        schemaTree.setRowHeight(22);

        JPopupMenu schemaPopup = new JPopupMenu();
        JMenuItem copyHiveItem = new JMenuItem("Copy as Hive DDL");
        JMenuItem copyJavaItem = new JMenuItem("Copy as Java POJO");

        copyHiveItem.addActionListener(e -> {
            if (currentSchema != null) copyToClipboard(SchemaCodeGenerator.generateHiveDDL(currentSchema, "parquet_table"));
        });
        copyJavaItem.addActionListener(e -> {
            if (currentSchema != null) copyToClipboard(SchemaCodeGenerator.generateJavaPojo(currentSchema, "ParquetRecord"));
        });

        schemaPopup.add(copyHiveItem);
        schemaPopup.add(copyJavaItem);
        schemaTree.setComponentPopupMenu(schemaPopup);

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

        metadataTable = new JTable();
        metadataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        metadataTable.setEnabled(false);
        metadataTable.setShowGrid(true);
        metadataTable.setGridColor(JBColor.LIGHT_GRAY);
        metadataTable.setRowHeight(24);
        JScrollPane metaScroll = new JScrollPane(metadataTable);

        // --- 数据表格：集成高性能 CRT 扫描线绘制 ---
        dataTable = new JTable() {
            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                if (isCRTMode) {
                    Graphics2D g2 = (Graphics2D) g.create();
                    g2.setColor(new Color(0, 255, 0, 18));
                    Rectangle clip = g.getClipBounds();
                    if (clip != null) {
                        int startY = clip.y - (clip.y % 3);
                        for (int y = startY; y < clip.y + clip.height; y += 3) {
                            g2.drawLine(clip.x, y, clip.x + clip.width, y);
                        }
                    }
                    g2.dispose();
                }
            }
        };
        dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

        // 注册数据行和表头渲染器
        SmartCellRenderer smartRenderer = new SmartCellRenderer();
        dataTable.setDefaultRenderer(Object.class, smartRenderer);
        dataTable.setDefaultRenderer(Integer.class, smartRenderer);
        dataTable.setDefaultRenderer(Long.class, smartRenderer);
        dataTable.setDefaultRenderer(Float.class, smartRenderer);
        dataTable.setDefaultRenderer(Double.class, smartRenderer);
        dataTable.setDefaultRenderer(Boolean.class, smartRenderer);

        // 接管表头渲染，但保留系统原生的排序图标处理能力
        TableCellRenderer defaultHeaderRenderer = dataTable.getTableHeader().getDefaultRenderer();
        dataTable.getTableHeader().setDefaultRenderer(new SmartHeaderRenderer(defaultHeaderRenderer));
        dataTable.getTableHeader().setOpaque(true);

        dataTable.setShowGrid(true);
        dataTable.setGridColor(new JBColor(new Color(220, 220, 220), new Color(80, 80, 80)));
        dataTable.setIntercellSpacing(new Dimension(1, 1));
        dataTable.setRowHeight(24);

        dataScrollPane = new JScrollPane(dataTable);

        JTable rowTable = new RowNumberTable(dataTable);
        dataScrollPane.setRowHeaderView(rowTable);
        dataScrollPane.setCorner(JScrollPane.UPPER_LEFT_CORNER, rowTable.getTableHeader());

        dataTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) { handleContextMenu(e); }
            @Override
            public void mouseReleased(MouseEvent e) { handleContextMenu(e); }

            private void handleContextMenu(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    JTable source = (JTable) e.getSource();
                    int row = source.rowAtPoint(e.getPoint());
                    int col = source.columnAtPoint(e.getPoint());

                    if (row >= 0 && col >= 0) {
                        source.setRowSelectionInterval(row, row);
                        source.setColumnSelectionInterval(col, col);

                        JPopupMenu popup = new JPopupMenu();
                        JMenuItem viewHexItem = new JMenuItem("View as Hex / Raw 🧐");
                        viewHexItem.addActionListener(actionEvent -> {
                            Object val = source.getValueAt(row, col);
                            showHexDialog(val);
                        });
                        popup.add(viewHexItem);
                        popup.show(e.getComponent(), e.getX(), e.getY());
                    }
                }
            }
        });

        // 过滤框与悬停提示
        dataFilterField = new HintTextField("e.g. name='Alice' AND age<30 — press Enter to apply");
        dataFilterField.setToolTipText("<html>Enter filter expression.<br><font color='gray'><i>Hidden protocols [rgb, crt, matrix, phil] are standing by...</i></font></html>");
        dataFilterField.addActionListener(e -> applyDataFilter());
        dataFilterField.setTransferHandler(null);

        JButton helpButton = new JButton("?");
        helpButton.addActionListener(e -> FilterHelpDialog.show(mainPanel));
        helpButton.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        JPanel filterBar = new JPanel(new BorderLayout(5, 5));
        filterBar.add(dataFilterField, BorderLayout.CENTER);
        filterBar.add(helpButton, BorderLayout.EAST);

        JPanel dataTabPanel = new JPanel(new BorderLayout());
        dataTabPanel.add(dataScrollPane, BorderLayout.CENTER);
        dataTabPanel.add(filterBar, BorderLayout.NORTH);

        tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Schema", schemaTabContainer);
        tabbedPane.addTab("Metadata", metaScroll);
        tabbedPane.addTab("Data", dataTabPanel);

        contentPanel = new JPanel(new BorderLayout());
        contentPanel.add(tabbedPane, BorderLayout.CENTER);
        wholeContentPanel.add(contentPanel, BorderLayout.CENTER);

        metadataTable.setDefaultRenderer(Object.class, new SmartCellRenderer());
        // ✨ 新增这三行：接管 metadataTable 的表头渲染
        TableCellRenderer defaultMetaHeaderRenderer = metadataTable.getTableHeader().getDefaultRenderer();
        metadataTable.getTableHeader().setDefaultRenderer(new SmartHeaderRenderer(defaultMetaHeaderRenderer));
        metadataTable.getTableHeader().setOpaque(true);
    }

    private void initBottomPanel() {
        prevButton = new JButton(AllIcons.Actions.Back);
        prevButton.setToolTipText("Previous Page");
        makeFlat(prevButton);

        nextButton = new JButton(AllIcons.Actions.Forward);
        nextButton.setToolTipText("Next Page");
        makeFlat(nextButton);

        pageInfoLabel = new JLabel("Page 0 of 0");
        showAllRowsCheckbox = new JCheckBox("Show all rows");
        showAllRowsCheckbox.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        exportFormatBox = new ComboBox<>(new String[]{"CSV", "JSON"});
        exportFormatBox.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        exportButton = new JButton("Export CSV", AllIcons.ToolbarDecorator.Export);
        exportButton.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        filterCountLabel = new JLabel("Showing 0 of 0 rows");
        totalRowLabel = new JLabel("Total rows: 0");

        JPanel bottomPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 10, 5, 10);
        gbc.fill = GridBagConstraints.NONE;

        JPanel leftGroup = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        leftGroup.add(prevButton);
        leftGroup.add(pageInfoLabel);
        leftGroup.add(nextButton);
        leftGroup.add(showAllRowsCheckbox);
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

    private void makeFlat(JButton btn) {
        btn.setBorderPainted(false);
        btn.setContentAreaFilled(false);
        btn.setFocusPainted(false);
        btn.setOpaque(false);
        btn.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
    }

    private void setupListeners() {
        prevButton.addActionListener(e -> changePage(-1));
        nextButton.addActionListener(e -> changePage(1));
        showAllRowsCheckbox.addActionListener(e -> reloadDataView());

        exportFormatBox.addActionListener(e -> {
            String selected = (String) exportFormatBox.getSelectedItem();
            String format = (selected == null ? "CSV" : selected.toUpperCase());
            exportButton.setText("Export " + format);
            exportButton.setToolTipText("Export as " + format);
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

        tabbedPane.addChangeListener(e -> {
            boolean isDataTab = tabbedPane.getSelectedIndex() == 2;
            boolean hasFile = currentFile != null;
            updateControlState(hasFile && isDataTab);
            if (hasFile && isDataTab) updatePaginationState();
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

        currentColumnStats = ParquetService.readColumnStats(file);

        showSchemaView();
        showMetadataView(metadata.extraMeta());

        tabbedPane.setSelectedIndex(0);
        updateControlState(false);

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
            buildSchemaTree(root, field, null);
        }
        schemaTree.setModel(new DefaultTreeModel(root));
        for (int i = 0; i < schemaTree.getRowCount(); i++) schemaTree.expandRow(i);
    }

    private void buildSchemaTree(DefaultMutableTreeNode parent, Type type, String parentPath) {
        String currentPath = (parentPath == null || parentPath.isEmpty())
                ? type.getName()
                : parentPath + "." + type.getName();

        String statsHtml = "";
        if (type.isPrimitive() && currentColumnStats != null) {
            ParquetService.ColumnDetails detail = currentColumnStats.get(currentPath);
            if (detail != null) {
                statsHtml = String.format(
                        "&nbsp;&nbsp;<font color='#e67e22' size='3'>[Size: %s]</font> <font color='#27ae60' size='3'>[Ratio: %s]</font> <font color='#7f8c8d' size='3'>[Nulls: %s]</font>",
                        humanReadableByteCount(detail.totalCompressedSize),
                        detail.getCompressionRatio(),
                        detail.getNullPercentage()
                );
            }
        }

        Color typeColorObj = new JBColor(new Color(0x0033B3), new Color(0x589DF6));
        String typeColor = toHex(typeColorObj);
        String attrColor = toHex(JBColor.GRAY);

        String nodeLabel;
        if (type.isPrimitive()) {
            String originalType = (type.getOriginalType() != null) ? " (" + type.getOriginalType() + ")" : "";
            nodeLabel = String.format("<html><b>%s</b> : <font color='%s'>%s</font>%s <font color='%s'>[%s]</font>%s</html>",
                    type.getName(), typeColor, type.asPrimitiveType().getPrimitiveTypeName(), originalType, attrColor, type.getRepetition(), statsHtml);
            parent.add(new DefaultMutableTreeNode(nodeLabel));
        } else {
            String originalType = (type.getOriginalType() != null) ? " (" + type.getOriginalType() + ")" : "";
            nodeLabel = String.format("<html><b>%s</b>%s <font color='%s'>[%s]</font></html>",
                    type.getName(), originalType, attrColor, type.getRepetition());
            DefaultMutableTreeNode groupNode = new DefaultMutableTreeNode(nodeLabel);
            parent.add(groupNode);
            for (Type child : type.asGroupType().getFields()) {
                buildSchemaTree(groupNode, child, currentPath);
            }
        }
    }

    private String toHex(Color color) {
        return String.format("#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }

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
        metadataTable.getColumnModel().getColumn(0).setPreferredWidth(150);
        metadataTable.getColumnModel().getColumn(1).setPreferredWidth(400);
        metadataTable.setDefaultRenderer(Object.class, new SmartCellRenderer());
    }

    private void showDataView(File file) throws IOException {
        List<String> selectedColumns = getSelectedColumns();
        if (selectedColumns.isEmpty()) return;

        List<Object[]> rows = ParquetService.readPageData(file, currentSchema, selectedColumns,
                currentPage, pageSize, showAllRowsCheckbox.isSelected());

        ParquetTableModel model = new ParquetTableModel(
                rows.toArray(new Object[0][]),
                selectedColumns.toArray(),
                currentSchema
        );

        // 核心修复：UI 更新必须回到 EDT 线程
        SwingUtilities.invokeLater(() -> {
            dataTable.setModel(model);

            JViewport rowHeader = dataScrollPane.getRowHeader();
            if (rowHeader != null && rowHeader.getView() instanceof RowNumberTable) {
                ((RowNumberTable) rowHeader.getView()).setModel(model);
            }

            applyDataFilter();

            for (int i = 0; i < dataTable.getColumnCount(); i++) {
                TableColumn column = dataTable.getColumnModel().getColumn(i);
                column.setMinWidth(40);
                column.setPreferredWidth(100);
            }

            updatePaginationState();
            updateFilterCountLabel();
        });
    }

    private void showHexDialog(Object val) {
        if (val == null) {
            JOptionPane.showMessageDialog(mainPanel, "Value is null (0x00)", "Hex View", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        byte[] bytes = val.toString().getBytes(StandardCharsets.UTF_8);
        StringBuilder hexBuilder = new StringBuilder();
        StringBuilder charBuilder = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            hexBuilder.append(String.format("%02X ", b));

            char c = (char) b;
            if (!Character.isISOControl(c)) charBuilder.append(c);
            else charBuilder.append('.');

            if ((i + 1) % 16 == 0) {
                hexBuilder.append("  |  ").append(charBuilder).append("\n");
                charBuilder.setLength(0);
            }
        }
        if (charBuilder.length() > 0) {
            while (charBuilder.length() < 16) {
                hexBuilder.append("   ");
                charBuilder.append(" ");
            }
            hexBuilder.append("  |  ").append(charBuilder).append("\n");
        }

        JTextArea area = new JTextArea(hexBuilder.toString());
        area.setFont(new Font("Monospaced", Font.PLAIN, 12));
        area.setEditable(false);
        JScrollPane scroll = new JScrollPane(area);
        scroll.setPreferredSize(new Dimension(500, 300));

        JOptionPane.showMessageDialog(mainPanel, scroll, "Hex / Raw View", JOptionPane.PLAIN_MESSAGE);
    }

    private void copyToClipboard(String text) {
        StringSelection selection = new StringSelection(text);
        Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
        JOptionPane.showMessageDialog(mainPanel, "Code copied to clipboard! 📋", "Copied", JOptionPane.INFORMATION_MESSAGE);
    }

    private String humanReadableByteCount(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp-1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    private void updatePaginationState() {
        if (tabbedPane.getSelectedIndex() != 2) {
            prevButton.setEnabled(false);
            nextButton.setEnabled(false);
            return;
        }
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
                    SwingUtilities.invokeLater(() -> showError("❌ Failed: " + ex.getMessage()));
                }
            }));
            debounceTimer.setRepeats(false);
            debounceTimer.start();
        }
    }

    private void applyDataFilter() {
        String text = dataFilterField.getText().trim();

        if ("rgb".equalsIgnoreCase(text)) {
            toggleRGBMode();
            dataFilterField.setText("");
            return;
        }

        if ("crt".equalsIgnoreCase(text) || "matrix".equalsIgnoreCase(text)) {
            toggleCRTMode();
            dataFilterField.setText("");
            return;
        }

        if ("phil".equalsIgnoreCase(text) || "author".equalsIgnoreCase(text)) {
            showAuthorCredits();
            dataFilterField.setText("");
            return;
        }

        try {
            DataFilterParser parser = new DataFilterParser(dataTable.getModel());
            RowFilter<TableModel, Integer> filter = parser.parse(text);
            TableRowSorter<TableModel> sorter = new TableRowSorter<>(dataTable.getModel());
            sorter.setRowFilter(filter);
            dataTable.setRowSorter(sorter);
            updateFilterCountLabel();
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(mainPanel,
                    e.getMessage(),
                    "Filter Error", JOptionPane.WARNING_MESSAGE);
        }
    }

    private void toggleRGBMode() {
        isRGBMode = !isRGBMode;
        if (isRGBMode) {
            if (isCRTMode) isCRTMode = false;
            rgbTimer.start();
            JOptionPane.showMessageDialog(mainPanel, "🌈 RGB Gamer Mode Activated! 🌈\nFPS Boosted +100%", "System Override", JOptionPane.INFORMATION_MESSAGE);
        } else {
            rgbTimer.stop();
            dataTable.repaint();
            if(dataTable.getTableHeader() != null) dataTable.getTableHeader().repaint();
            JOptionPane.showMessageDialog(mainPanel, "RGB Mode Deactivated.", "System", JOptionPane.PLAIN_MESSAGE);
        }
    }

    private void toggleCRTMode() {
        isCRTMode = !isCRTMode;
        if (isCRTMode) {
            if (isRGBMode) {
                isRGBMode = false;
                rgbTimer.stop();
            }
            dataTable.repaint();
            if(dataTable.getTableHeader() != null) dataTable.getTableHeader().repaint();
            JOptionPane.showMessageDialog(mainPanel, "Wake up, Neo...\nMatrix CRT Mode Activated 📟", "System Override", JOptionPane.INFORMATION_MESSAGE);
        } else {
            dataTable.repaint();
            if(dataTable.getTableHeader() != null) dataTable.getTableHeader().repaint();
            JOptionPane.showMessageDialog(mainPanel, "CRT Mode Deactivated.", "System", JOptionPane.PLAIN_MESSAGE);
        }
    }

    private void showAuthorCredits() {
        String art = "<html><pre> ____  _     _ _ \n|  _ \\| |__ (_) |\n| |_) | '_ \\| | |\n|  __/| | | | | |\n|_|   |_| |_|_|_|\n\nCreated by Phil Zhang\nThe Parquet Wizard \uD83E\uDDD9\u200D♂️</pre></html>";
        JOptionPane.showMessageDialog(mainPanel, art, "About the Author", JOptionPane.PLAIN_MESSAGE);
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
        if (icon != null && icon.getImage() != null) content.add(new JLabel(new ImageIcon(icon.getImage().getScaledInstance(32, 32, Image.SCALE_DEFAULT))), BorderLayout.WEST);
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

    private static class ParquetTableModel extends DefaultTableModel {
        private final MessageType schema;
        private final String[] columns;

        public ParquetTableModel(Object[][] data, Object[] columnNames, MessageType schema) {
            super(data, columnNames);
            this.schema = schema;
            this.columns = new String[columnNames.length];
            for(int i=0; i<columnNames.length; i++) this.columns[i] = columnNames[i].toString();
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            try {
                String colName = columns[columnIndex];
                if (schema.containsField(colName)) {
                    Type type = schema.getType(colName);
                    if (type.isPrimitive() && !type.isRepetition(Type.Repetition.REPEATED)) {
                        PrimitiveType pt = type.asPrimitiveType();
                        switch (pt.getPrimitiveTypeName()) {
                            case INT32: return Integer.class;
                            case INT64: return Long.class;
                            case FLOAT: return Float.class;
                            case DOUBLE: return Double.class;
                            case BOOLEAN: return Boolean.class;
                        }
                    }
                }
            } catch (Exception e) {}
            return Object.class;
        }
    }

    // --- 表头渲染器 ---
    // --- 核心修复：使用装饰器模式保留原生排序箭头 ---
    private class SmartHeaderRenderer implements TableCellRenderer {
        private final TableCellRenderer defaultRenderer;

        // 接收系统原生的 renderer 作为委托对象
        public SmartHeaderRenderer(TableCellRenderer defaultRenderer) {
            this.defaultRenderer = defaultRenderer;
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus,
                                                       int row, int column) {
            // 1. 让系统原生渲染器先处理（它会负责加上排序的上下箭头图标、边框等）
            Component comp = defaultRenderer.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

            if (comp instanceof JLabel) {
                JLabel label = (JLabel) comp;

                // 2. 强制文字居中对齐
                label.setHorizontalAlignment(SwingConstants.CENTER);

                // 3. 注入极客彩蛋特效
                if (isRGBMode) {
                    float hue = hueOffset + (column * 0.03f);
                    label.setBackground(new Color(30, 33, 36));
                    label.setForeground(Color.getHSBColor(hue, 0.7f, 1.0f));
                    label.setOpaque(true); // 必须设置为 true 才能显示背景色
                } else if (isCRTMode) {
                    label.setBackground(new Color(2, 8, 2));
                    label.setForeground(new Color(50, 255, 50));
                    label.setOpaque(true);
                } else {
                    // 正常模式：恢复 IDEA 系统默认的主题色，不做过多干预
                    label.setForeground(UIManager.getColor("TableHeader.foreground"));
                    // 这里不强制 setOpaque，让 Darcula 或 IntelliJ 主题自己接管背景和透明度
                }
            }
            return comp;
        }
    }

    // --- 数据行渲染器 ---
    private class SmartCellRenderer extends DefaultTableCellRenderer {
        private final Color nullColor = JBColor.GRAY;
        private final Color evenRowColor = new JBColor(new Color(245, 248, 250), new Color(60, 63, 65));

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus,
                                                       int row, int column) {
            super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

            if (isRGBMode) {
                float hue = hueOffset + (row * 0.05f);
                Color rainbowColor = Color.getHSBColor(hue, 0.4f, 1.0f);
                if (isSelected) {
                    setBackground(table.getSelectionBackground());
                    setForeground(table.getSelectionForeground());
                } else {
                    setBackground(rainbowColor);
                    setForeground(Color.BLACK);
                }
            } else if (isCRTMode) {
                Color crtBg = new Color(5, 18, 5);
                Color crtFg = new Color(50, 255, 50);
                if (isSelected) {
                    setBackground(crtFg);
                    setForeground(crtBg);
                } else {
                    setBackground(crtBg);
                    setForeground(crtFg);
                }
            } else {
                if (!isSelected) {
                    setBackground(row % 2 == 0 ? table.getBackground() : evenRowColor);
                    setForeground(table.getForeground());
                } else {
                    setBackground(table.getSelectionBackground());
                    setForeground(table.getSelectionForeground());
                }
            }

            if (value == null || value.toString().isEmpty()) {
                setText("<null>");
                if (!isSelected && !isRGBMode && !isCRTMode) setForeground(nullColor);
            } else {
                setText(value.toString());
            }

            setHorizontalAlignment(SwingConstants.LEFT);
            return this;
        }
    }

    private static class RowNumberTable extends JTable {
        private final JTable mainTable;

        public RowNumberTable(JTable mainTable) {
            this.mainTable = mainTable;
            setAutoCreateColumnsFromModel(false);
            setModel(mainTable.getModel());
            setSelectionModel(mainTable.getSelectionModel());
            setRowHeight(mainTable.getRowHeight());
            getColumnModel().addColumn(new TableColumn());
            getColumnModel().getColumn(0).setCellRenderer(new RowNumberRenderer());
            setPreferredScrollableViewportSize(new Dimension(40, 0));
            setFocusable(false);
            setShowGrid(false);
            setIntercellSpacing(new Dimension(0, 0));
        }

        @Override
        public int getRowCount() { return mainTable.getRowCount(); }

        @Override
        public boolean isCellEditable(int row, int column) { return false; }

        private static class RowNumberRenderer extends DefaultTableCellRenderer {
            public RowNumberRenderer() {
                setHorizontalAlignment(JLabel.CENTER);
                setBackground(JBColor.PanelBackground);
                setForeground(JBColor.GRAY);
            }
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
                setText(String.valueOf(row + 1));
                return this;
            }
        }
    }
}