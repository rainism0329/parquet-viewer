package com.bigphil.parquetviewer;

import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.ComboBox;
import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.components.JBLabel;
import com.intellij.util.ui.FormBuilder;
import org.jetbrains.annotations.Nullable;

import javax.swing.JComponent;
import javax.swing.JPanel;

final class ExportOptionsDialog extends DialogWrapper {
    enum Format { CSV, JSON }
    enum Scope {
        CURRENT_VIEW("Current loaded and filtered rows"),
        WHOLE_FILE("Whole file");

        private final String label;
        Scope(String label) { this.label = label; }
        @Override public String toString() { return label; }
    }

    private final ComboBox<Format> formatBox = new ComboBox<>(Format.values());
    private final ComboBox<Scope> scopeBox = new ComboBox<>(Scope.values());
    private final int visibleColumnCount;

    ExportOptionsDialog(Project project, int visibleColumnCount, boolean virtualMode) {
        super(project, true);
        this.visibleColumnCount = visibleColumnCount;
        setTitle("Export Parquet Data");
        setOKButtonText("Choose Destination…");
        if (virtualMode) scopeBox.setSelectedItem(Scope.WHOLE_FILE);
        init();
    }

    Format format() { return (Format) formatBox.getSelectedItem(); }
    Scope scope() { return (Scope) scopeBox.getSelectedItem(); }

    @Override
    protected @Nullable JComponent createCenterPanel() {
        JPanel panel = FormBuilder.createFormBuilder()
                .addLabeledComponent("Format:", formatBox)
                .addLabeledComponent("Rows:", scopeBox)
                .addLabeledComponent("Columns:", new JBLabel(visibleColumnCount + " visible columns"))
                .addComponentFillVertically(new JPanel(), 0)
                .getPanel();
        panel.setPreferredSize(new java.awt.Dimension(440, 150));
        return panel;
    }
}
