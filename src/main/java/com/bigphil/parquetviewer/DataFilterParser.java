package com.bigphil.parquetviewer;

import org.mvel2.MVEL;

import javax.swing.RowFilter;
import javax.swing.table.TableModel;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataFilterParser {
    private final TableModel model;
    private final List<ColumnAlias> columnAliases;

    public DataFilterParser(TableModel model) {
        this.model = model;
        this.columnAliases = new ArrayList<>(model.getColumnCount());
        for (int column = 0; column < model.getColumnCount(); column++) {
            columnAliases.add(new ColumnAlias(model.getColumnName(column), "__pv_col_" + column, column));
        }
        columnAliases.sort(Comparator.comparingInt((ColumnAlias alias) -> alias.columnName().length()).reversed());
    }

    public RowFilter<TableModel, Integer> parse(String filterText) {
        if (filterText == null || filterText.trim().isEmpty()) {
            return null;
        }

        String preprocessed = aliasColumnReferences(MvelExpressionPreprocessor.preprocess(filterText));

        // Compile once and reuse the compiled expression for every row. The previous
        // implementation validated with compileExpression(), but then called MVEL.eval()
        // for every row, which parsed the same expression repeatedly.
        final Serializable compiledExpression;
        try {
            compiledExpression = MVEL.compileExpression(preprocessed);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "\"" + filterText + "\" is not a valid filter expression.\n\n" +
                    "Click the ? button for supported filter syntax.", e);
        }

        return new RowFilter<>() {
            public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                Map<String, Object> rowVars = new HashMap<>();
                for (ColumnAlias alias : columnAliases) {
                    rowVars.put(alias.alias(), entry.getValue(alias.modelIndex()));
                }

                try {
                    Object result = MVEL.executeExpression(compiledExpression, rowVars);
                    return Boolean.TRUE.equals(result);
                } catch (Exception e) {
                    return false;
                }
            }
        };
    }

    /**
     * MVEL treats dots and hyphens as operators. Replace column references outside
     * quoted literals with generated safe identifiers, preferring the longest column
     * name so overlapping names remain deterministic.
     */
    private String aliasColumnReferences(String expression) {
        StringBuilder result = new StringBuilder(expression.length() + 16);
        boolean singleQuoted = false;
        boolean doubleQuoted = false;

        for (int index = 0; index < expression.length();) {
            char current = expression.charAt(index);
            if (current == '\\' && (singleQuoted || doubleQuoted) && index + 1 < expression.length()) {
                result.append(current).append(expression.charAt(index + 1));
                index += 2;
                continue;
            }
            if (current == '\'' && !doubleQuoted) {
                singleQuoted = !singleQuoted;
                result.append(current);
                index++;
                continue;
            }
            if (current == '"' && !singleQuoted) {
                doubleQuoted = !doubleQuoted;
                result.append(current);
                index++;
                continue;
            }

            ColumnAlias match = null;
            if (!singleQuoted && !doubleQuoted && (index == 0 || expression.charAt(index - 1) != '.')) {
                for (ColumnAlias alias : columnAliases) {
                    if (matchesColumnAt(expression, index, alias.columnName())) {
                        match = alias;
                        break;
                    }
                }
            }
            if (match != null) {
                result.append(match.alias());
                index += match.columnName().length();
            } else {
                result.append(current);
                index++;
            }
        }
        return result.toString();
    }

    private static boolean matchesColumnAt(String expression, int index, String columnName) {
        if (columnName.isEmpty() || index + columnName.length() > expression.length()) return false;
        if (!expression.regionMatches(index, columnName, 0, columnName.length())) return false;
        int before = index - 1;
        int after = index + columnName.length();
        return (before < 0 || !isIdentifierPart(expression.charAt(before)))
                && (after >= expression.length() || !isIdentifierPart(expression.charAt(after)));
    }

    private static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_' || value == '$';
    }

    private record ColumnAlias(String columnName, String alias, int modelIndex) {}
}
