package com.bigphil.parquetviewer;

import com.intellij.ui.JBColor;

import javax.swing.BorderFactory;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.Component;
import java.awt.Font;

/** Theme-aware renderer that distinguishes null, empty strings and loading cells. */
public final class ParquetCellRenderer extends DefaultTableCellRenderer {
    private static final String NULL_TEXT = "NULL";
    private static final String EMPTY_TEXT = "\"\"";

    public ParquetCellRenderer() {
        setBorder(BorderFactory.createEmptyBorder(0, 6, 0, 6));
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
                                                   boolean selected, boolean focused,
                                                   int row, int column) {
        super.getTableCellRendererComponent(table, value, selected, focused, row, column);
        setFont(table.getFont());
        setToolTipText(null);

        if (value == VirtualParquetTableModel.LoadingValue.INSTANCE) {
            setText("Loading…");
            setFont(getFont().deriveFont(Font.ITALIC));
            if (!selected) setForeground(JBColor.GRAY);
            setHorizontalAlignment(SwingConstants.LEFT);
            return this;
        }

        if (value == null) {
            setText(NULL_TEXT);
            setFont(getFont().deriveFont(Font.ITALIC));
            if (!selected) setForeground(JBColor.GRAY);
            setHorizontalAlignment(SwingConstants.LEFT);
            return this;
        }

        if (value instanceof String stringValue && stringValue.isEmpty()) {
            setText(EMPTY_TEXT);
            if (!selected) setForeground(JBColor.GRAY);
            setHorizontalAlignment(SwingConstants.LEFT);
            return this;
        }

        String text = String.valueOf(value);
        setText(text);
        if (value instanceof Number) setHorizontalAlignment(SwingConstants.RIGHT);
        else if (value instanceof Boolean) setHorizontalAlignment(SwingConstants.CENTER);
        else setHorizontalAlignment(SwingConstants.LEFT);
        if (text.length() > 40) setToolTipText("<html><body style='width:500px'>" + escapeHtml(text) + "</body></html>");
        return this;
    }

    private static String escapeHtml(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("\n", "<br>");
    }
}
