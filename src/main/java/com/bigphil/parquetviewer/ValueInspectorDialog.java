package com.bigphil.parquetviewer;

import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.ui.components.JBTabbedPane;
import com.intellij.ui.components.JBTextArea;
import com.intellij.util.ui.JBUI;
import org.jetbrains.annotations.Nullable;

import javax.swing.JComponent;
import java.awt.Dimension;
import java.nio.charset.StandardCharsets;

/** Full text and UTF-8 hexadecimal inspection for a selected cell value. */
final class ValueInspectorDialog extends DialogWrapper {
    private final Object value;

    ValueInspectorDialog(String columnName, Object value) {
        super(true);
        this.value = value;
        setTitle("Value Inspector · " + columnName);
        setOKButtonText("Close");
        init();
    }

    @Override
    protected @Nullable JComponent createCenterPanel() {
        JBTabbedPane tabs = new JBTabbedPane();
        tabs.addTab("Value", scrollableText(value == null ? "NULL" : String.valueOf(value)));
        tabs.addTab("UTF-8 Hex", scrollableText(toHex(value)));
        tabs.setPreferredSize(new Dimension(JBUI.scale(720), JBUI.scale(480)));
        return tabs;
    }

    private static JBScrollPane scrollableText(String text) {
        JBTextArea area = new JBTextArea(text);
        area.setEditable(false);
        area.setLineWrap(false);
        area.setFont(new java.awt.Font(java.awt.Font.MONOSPACED, java.awt.Font.PLAIN, area.getFont().getSize()));
        area.setCaretPosition(0);
        return new JBScrollPane(area);
    }

    private static String toHex(Object value) {
        if (value == null) return "NULL has no byte representation.";
        byte[] bytes = String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        StringBuilder result = new StringBuilder();
        for (int offset = 0; offset < bytes.length; offset += 16) {
            result.append(String.format("%08X  ", offset));
            StringBuilder characters = new StringBuilder(16);
            for (int index = 0; index < 16; index++) {
                int position = offset + index;
                if (position < bytes.length) {
                    int unsigned = bytes[position] & 0xFF;
                    result.append(String.format("%02X ", unsigned));
                    characters.append(unsigned >= 32 && unsigned < 127 ? (char) unsigned : '.');
                } else {
                    result.append("   ");
                    characters.append(' ');
                }
                if (index == 7) result.append(' ');
            }
            result.append(" | ").append(characters).append('\n');
        }
        return result.toString();
    }
}
