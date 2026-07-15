package com.bigphil.parquetviewer;

import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.util.ui.JBUI;
import org.jetbrains.annotations.Nullable;

import javax.swing.JComponent;
import javax.swing.JEditorPane;
import java.awt.Component;
import java.awt.Dimension;

/** Focused filter help. Author and donation content intentionally live outside task help. */
public final class FilterHelpDialog {
    private FilterHelpDialog() {}

    public static void show(Component parent) {
        new HelpDialog().show();
    }

    private static final class HelpDialog extends DialogWrapper {
        private HelpDialog() {
            super(true);
            setTitle("Parquet Filter Syntax");
            setOKButtonText("Close");
            init();
        }

        @Override
        protected @Nullable JComponent createCenterPanel() {
            JEditorPane content = new JEditorPane("text/html", """
                    <html><body style='font-family:sans-serif'>
                    <h2>Filter loaded rows</h2>
                    <p>Filters are applied to the rows currently loaded in the data grid.
                    String comparisons are case-sensitive.</p>
                    <table cellpadding='6' cellspacing='0'>
                      <tr><td><b>name = 'Alice'</b></td><td>Equals</td></tr>
                      <tr><td><b>age != 30</b></td><td>Not equal</td></tr>
                      <tr><td><b>score &gt;= 80</b></td><td>Numeric comparison</td></tr>
                      <tr><td><b>country IN ('US', 'UK')</b></td><td>Match a list</td></tr>
                      <tr><td><b>status NOT IN ('X', 'Y')</b></td><td>Exclude a list</td></tr>
                      <tr><td><b>email IS NULL</b></td><td>Null value</td></tr>
                      <tr><td><b>email IS NOT NULL</b></td><td>Non-null value</td></tr>
                      <tr><td><b>name LIKE '%son'</b></td><td>Pattern match</td></tr>
                      <tr><td><b>name='Alice' AND age&lt;30</b></td><td>Combine conditions</td></tr>
                      <tr><td><b>(region='EU' OR region='US')</b></td><td>Group conditions</td></tr>
                    </table>
                    <h3>Notes</h3>
                    <ul>
                      <li>Put string values in single quotes.</li>
                      <li>Use AND, OR and parentheses to combine expressions.</li>
                      <li>Press Enter to apply a filter. Use the history button to reuse one.</li>
                    </ul>
                    </body></html>
                    """);
            content.setEditable(false);
            content.setCaretPosition(0);
            JBScrollPane scrollPane = new JBScrollPane(content);
            scrollPane.setPreferredSize(new Dimension(JBUI.scale(620), JBUI.scale(460)));
            return scrollPane;
        }
    }
}
