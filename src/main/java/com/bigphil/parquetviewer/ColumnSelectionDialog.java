package com.bigphil.parquetviewer;

import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.SearchTextField;
import com.intellij.ui.components.JBCheckBox;
import com.intellij.ui.components.JBLabel;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.util.ui.JBUI;
import org.jetbrains.annotations.Nullable;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Searchable, transactional column selection. No file reload occurs until OK. */
public final class ColumnSelectionDialog extends DialogWrapper {
    private final List<String> columns;
    private final Map<String, JBCheckBox> checkBoxes = new LinkedHashMap<>();
    private final JPanel checkBoxPanel = new JPanel();
    private final JBLabel countLabel = new JBLabel();

    public ColumnSelectionDialog(Project project, List<String> columns, List<String> selectedColumns) {
        super(project, true);
        this.columns = List.copyOf(columns);
        setTitle("Choose Columns");
        setOKButtonText("Apply");

        checkBoxPanel.setLayout(new BoxLayout(checkBoxPanel, BoxLayout.Y_AXIS));
        for (String column : columns) {
            JBCheckBox checkBox = new JBCheckBox(column, selectedColumns.contains(column));
            checkBox.addActionListener(event -> updateCount());
            checkBoxes.put(column, checkBox);
            checkBoxPanel.add(checkBox);
        }
        updateCount();
        init();
    }

    public List<String> selectedColumns() {
        List<String> selected = new ArrayList<>();
        for (String column : columns) {
            JBCheckBox checkBox = checkBoxes.get(column);
            if (checkBox != null && checkBox.isSelected()) selected.add(column);
        }
        return selected;
    }

    @Override
    protected @Nullable JComponent createCenterPanel() {
        JPanel root = new JPanel(new BorderLayout(JBUI.scale(8), JBUI.scale(8)));
        root.setPreferredSize(new Dimension(JBUI.scale(420), JBUI.scale(520)));
        root.setBorder(JBUI.Borders.empty(8));

        SearchTextField searchField = new SearchTextField(false);
        searchField.getTextEditor().getEmptyText().setText("Search columns");
        searchField.addDocumentListener(new javax.swing.event.DocumentListener() {
            @Override public void insertUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
            @Override public void removeUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
            @Override public void changedUpdate(javax.swing.event.DocumentEvent e) { filter(searchField.getText()); }
        });
        root.add(searchField, BorderLayout.NORTH);

        JBScrollPane scrollPane = new JBScrollPane(checkBoxPanel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        root.add(scrollPane, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, JBUI.scale(6), 0));
        JButton selectAll = new JButton("Select All");
        JButton clear = new JButton("Clear");
        JButton invert = new JButton("Invert");
        selectAll.addActionListener(event -> setAll(true));
        clear.addActionListener(event -> setAll(false));
        invert.addActionListener(event -> {
            checkBoxes.values().forEach(box -> box.setSelected(!box.isSelected()));
            updateCount();
        });
        actions.add(selectAll);
        actions.add(clear);
        actions.add(invert);
        actions.add(countLabel);
        root.add(actions, BorderLayout.SOUTH);
        return root;
    }

    @Override
    protected void doOKAction() {
        if (selectedColumns().isEmpty()) {
            setErrorText("Select at least one column.");
            return;
        }
        super.doOKAction();
    }

    private void filter(String query) {
        String normalized = query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
        checkBoxes.forEach((column, box) -> box.setVisible(
                normalized.isEmpty() || column.toLowerCase(Locale.ROOT).contains(normalized)
        ));
        checkBoxPanel.revalidate();
        checkBoxPanel.repaint();
    }

    private void setAll(boolean selected) {
        checkBoxes.values().stream().filter(JComponent::isVisible).forEach(box -> box.setSelected(selected));
        updateCount();
    }

    private void updateCount() {
        long selected = checkBoxes.values().stream().filter(JBCheckBox::isSelected).count();
        countLabel.setText(String.format("%,d of %,d selected", selected, columns.size()));
    }
}
