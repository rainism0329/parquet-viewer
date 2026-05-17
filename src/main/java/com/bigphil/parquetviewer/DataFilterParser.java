package com.bigphil.parquetviewer;

import org.mvel2.MVEL;

import javax.swing.RowFilter;
import javax.swing.table.TableModel;
import java.util.HashMap;
import java.util.Map;

public class DataFilterParser {
    private final TableModel model;

    public DataFilterParser(TableModel model) {
        this.model = model;
    }

    public RowFilter<TableModel, Integer> parse(String filterText) {
        if (filterText == null || filterText.trim().isEmpty()) {
            return null;
        }

        String preprocessed = MvelExpressionPreprocessor.preprocess(filterText);

        // Validate syntax early, before applying to rows
        try {
            MVEL.compileExpression(preprocessed);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "\"" + filterText + "\" is not a valid filter expression.\n\n" +
                    "Click the ? button for supported filter syntax.", e);
        }

        return new RowFilter<>() {
            public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                Map<String, Object> rowVars = new HashMap<>();
                for (int i = 0; i < model.getColumnCount(); i++) {
                    rowVars.put(model.getColumnName(i), entry.getValue(i));
                }

                try {
                    Object result = MVEL.eval(preprocessed, rowVars);
                    return Boolean.TRUE.equals(result);
                } catch (Exception e) {
                    return false;
                }
            }
        };
    }
}
