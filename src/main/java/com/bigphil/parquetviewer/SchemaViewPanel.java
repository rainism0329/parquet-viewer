package com.bigphil.parquetviewer;

import com.intellij.openapi.ide.CopyPasteManager;
import com.intellij.ui.SearchTextField;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.ui.components.JBTextArea;
import com.intellij.ui.treeStructure.Tree;
import com.intellij.util.ui.JBUI;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JToggleButton;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.FlowLayout;
import java.awt.datatransfer.StringSelection;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

/** Structured and text schema views with search, statistics and code generation. */
public final class SchemaViewPanel {
    private static final String TREE_CARD = "tree";
    private static final String TEXT_CARD = "text";

    private final JPanel root = new JPanel(new BorderLayout());
    private final Tree tree = new Tree(new DefaultMutableTreeNode("No schema loaded"));
    private final JBTextArea textArea = new JBTextArea();
    private final CardLayout cardLayout = new CardLayout();
    private final JPanel cards = new JPanel(cardLayout);
    private final Consumer<String> statusConsumer;
    private MessageType schema;
    private Map<String, ParquetService.ColumnDetails> statistics = Map.of();

    public SchemaViewPanel(Consumer<String> statusConsumer) {
        this.statusConsumer = statusConsumer;
        textArea.setEditable(false);
        textArea.setLineWrap(false);
        tree.setRootVisible(true);

        cards.add(new JBScrollPane(tree), TREE_CARD);
        cards.add(new JBScrollPane(textArea), TEXT_CARD);
        root.add(createToolbar(), BorderLayout.NORTH);
        root.add(cards, BorderLayout.CENTER);
    }

    public JPanel component() {
        return root;
    }

    public void setSummary(ParquetService.ParquetFileSummary summary) {
        schema = summary.schema();
        statistics = summary.columnStats();
        textArea.setText(schema.toString());
        textArea.setCaretPosition(0);
        rebuildTree("");
    }

    private JPanel createToolbar() {
        JPanel toolbar = new JPanel(new BorderLayout(JBUI.scale(8), 0));
        toolbar.setBorder(JBUI.Borders.empty(5, 8));

        SearchTextField searchField = new SearchTextField(false);
        searchField.getTextEditor().getEmptyText().setText("Search schema");
        searchField.addDocumentListener(new javax.swing.event.DocumentListener() {
            @Override public void insertUpdate(javax.swing.event.DocumentEvent e) { rebuildTree(searchField.getText()); }
            @Override public void removeUpdate(javax.swing.event.DocumentEvent e) { rebuildTree(searchField.getText()); }
            @Override public void changedUpdate(javax.swing.event.DocumentEvent e) { rebuildTree(searchField.getText()); }
        });
        toolbar.add(searchField, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, JBUI.scale(4), 0));
        JToggleButton textMode = new JToggleButton("Text");
        textMode.addActionListener(event -> cardLayout.show(cards, textMode.isSelected() ? TEXT_CARD : TREE_CARD));
        JButton expand = new JButton("Expand");
        expand.addActionListener(event -> expandAll(true));
        JButton collapse = new JButton("Collapse");
        collapse.addActionListener(event -> expandAll(false));
        JButton hive = new JButton("Copy Hive DDL");
        hive.addActionListener(event -> copyGenerated(true));
        JButton java = new JButton("Copy Java POJO");
        java.addActionListener(event -> copyGenerated(false));
        actions.add(textMode);
        actions.add(expand);
        actions.add(collapse);
        actions.add(hive);
        actions.add(java);
        toolbar.add(actions, BorderLayout.EAST);
        return toolbar;
    }

    private void rebuildTree(String query) {
        if (schema == null) return;
        String normalized = query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
        DefaultMutableTreeNode rootNode = new DefaultMutableTreeNode("Schema: " + schema.getName());
        for (Type field : schema.getFields()) addTypeNode(rootNode, field, "", normalized);
        tree.setModel(new DefaultTreeModel(rootNode));
        if (normalized.isEmpty()) {
            tree.expandRow(0);
        } else {
            expandAll(true);
        }
    }

    private boolean addTypeNode(DefaultMutableTreeNode parent, Type type, String parentPath, String query) {
        String path = parentPath.isEmpty() ? type.getName() : parentPath + "." + type.getName();
        DefaultMutableTreeNode node = new DefaultMutableTreeNode(labelFor(type, path));
        boolean childMatches = false;
        if (!type.isPrimitive()) {
            for (Type child : type.asGroupType().getFields()) {
                childMatches |= addTypeNode(node, child, path, query);
            }
        }
        boolean selfMatches = query.isEmpty()
                || path.toLowerCase(Locale.ROOT).contains(query)
                || type.toString().toLowerCase(Locale.ROOT).contains(query);
        if (selfMatches || childMatches) parent.add(node);
        return selfMatches || childMatches;
    }

    private String labelFor(Type type, String path) {
        String typeName = type.isPrimitive()
                ? type.asPrimitiveType().getPrimitiveTypeName().name()
                : "GROUP";
        String logical = type.getLogicalTypeAnnotation() == null
                ? ""
                : " (" + type.getLogicalTypeAnnotation() + ")";
        StringBuilder label = new StringBuilder(type.getName())
                .append(" : ").append(typeName).append(logical)
                .append("  [").append(type.getRepetition()).append(']');
        ParquetService.ColumnDetails details = statistics.get(path);
        if (details != null) {
            label.append("  · ").append(humanBytes(details.totalCompressedSize))
                    .append(" · ").append(details.getCompressionRatio())
                    .append(" · ").append(details.getNullPercentage()).append(" null");
        }
        return label.toString();
    }

    private void expandAll(boolean expand) {
        if (expand) {
            for (int row = 0; row < tree.getRowCount(); row++) tree.expandRow(row);
        } else {
            for (int row = tree.getRowCount() - 1; row > 0; row--) tree.collapseRow(row);
        }
    }

    private void copyGenerated(boolean hive) {
        if (schema == null) return;
        String code = hive
                ? SchemaCodeGenerator.generateHiveDDL(schema, "parquet_table")
                : SchemaCodeGenerator.generateJavaPojo(schema, "ParquetRecord");
        CopyPasteManager.getInstance().setContents(new StringSelection(code));
        statusConsumer.accept(hive ? "Hive DDL copied" : "Java POJO copied");
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int unit = (int) (Math.log(bytes) / Math.log(1024));
        String prefix = "KMGTPE".substring(unit - 1, unit);
        return String.format(Locale.ROOT, "%.1f %sB", bytes / Math.pow(1024, unit), prefix);
    }
}
