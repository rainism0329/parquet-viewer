package com.bigphil.parquetviewer;

import javax.swing.RowFilter;
import javax.swing.table.TableModel;
import java.util.*;
import java.util.regex.Pattern;

public class DataFilterParser {
    private final TableModel model;

    public DataFilterParser(TableModel model) {
        this.model = model;
    }

    public RowFilter<TableModel, Integer> parse(String filterText) {
        if (filterText == null || filterText.trim().isEmpty()) {
            return null;
        }

        String[] andParts = filterText.split("(?i)\\s+AND\\s+");
        List<RowFilter<TableModel, Integer>> andFilters = new ArrayList<>();

        for (String andPart : andParts) {
            String[] orParts = andPart.split("(?i)\\s+OR\\s+");
            List<RowFilter<TableModel, Integer>> orFilters = new ArrayList<>();

            for (String expr : orParts) {
                RowFilter<TableModel, Integer> f = parseSingleFilter(expr.trim());
                if (f != null) {
                    orFilters.add(f);
                }
            }

            if (!orFilters.isEmpty()) {
                if (orFilters.size() == 1) {
                    andFilters.add(orFilters.get(0));
                } else {
                    andFilters.add(RowFilter.orFilter(orFilters));
                }
            }
        }

        if (andFilters.isEmpty()) return null;
        if (andFilters.size() == 1) return andFilters.get(0);
        return RowFilter.andFilter(andFilters);
    }

    private RowFilter<TableModel, Integer> parseSingleFilter(String expr) {
        expr = expr.trim();

        if (expr.matches("(?i)^\\w+\\s+IS\\s+NULL$")) {
            String colName = expr.split("\\s+")[0];
            int col = getColumnIndex(colName);
            if (col == -1) return null;
            return new RowFilter<>() {
                public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                    Object val = entry.getValue(col);
                    return val == null || val.toString().isEmpty();
                }
            };
        }

        if (expr.matches("(?i)^\\w+\\s+IS\\s+NOT\\s+NULL$")) {
            String colName = expr.split("\\s+")[0];
            int col = getColumnIndex(colName);
            if (col == -1) return null;
            return new RowFilter<>() {
                public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                    Object val = entry.getValue(col);
                    return val != null && !val.toString().isEmpty();
                }
            };
        }

        // Operators
        String[][] ops = {
                {"!=", "!="},
                {">=", ">="},
                {"<=", "<="},
                {">", ">"},
                {"<", "<"},
                {"~", "~"},
                {"=", "="},
                {" LIKE ", "LIKE"},
                {" NOT LIKE ", "NOT LIKE"},
                {" IN ", "IN"},
                {" NOT IN ", "NOT IN"}
        };

        for (String[] op : ops) {
            int idx = expr.toUpperCase().indexOf(op[0]);
            if (idx > 0) {
                String left = expr.substring(0, idx).trim();
                String right = expr.substring(idx + op[0].length()).trim();
                return buildFilter(left, op[1], right);
            }
        }

        return null;
    }

    private RowFilter<TableModel, Integer> buildFilter(String columnName, String operator, String value) {
        int col = getColumnIndex(columnName);
        if (col == -1) return null;

        switch (operator.toUpperCase()) {
            case "=":
                return RowFilter.regexFilter("(?i)^" + Pattern.quote(value) + "$", col);
            case "!=":
                return RowFilter.notFilter(RowFilter.regexFilter("(?i)^" + Pattern.quote(value) + "$", col));
            case ">":
                return numericFilter(col, value, (a, b) -> a > b);
            case "<":
                return numericFilter(col, value, (a, b) -> a < b);
            case ">=":
                return numericFilter(col, value, (a, b) -> a >= b);
            case "<=":
                return numericFilter(col, value, (a, b) -> a <= b);
            case "~":
                return RowFilter.regexFilter("(?i)" + Pattern.quote(value), col);
            case "LIKE":
                return RowFilter.regexFilter("(?i)^" + value.replace("%", ".*") + "$", col);
            case "NOT LIKE":
                return RowFilter.notFilter(RowFilter.regexFilter("(?i)^" + value.replace("%", ".*") + "$", col));
            case "IN":
                return buildInFilter(col, value, true);
            case "NOT IN":
                return buildInFilter(col, value, false);
            default:
                return null;
        }
    }

    private RowFilter<TableModel, Integer> numericFilter(int col, String val, java.util.function.BiPredicate<Double, Double> cmp) {
        try {
            double target = Double.parseDouble(val);
            return new RowFilter<>() {
                public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                    try {
                        return cmp.test(Double.parseDouble(entry.getValue(col).toString()), target);
                    } catch (Exception e) {
                        return false;
                    }
                }
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private RowFilter<TableModel, Integer> buildInFilter(int col, String val, boolean include) {
        String[] items = val.replaceAll("[()]", "").split(",");
        Set<String> set = new HashSet<>();
        for (String s : items) set.add(s.trim().toLowerCase());

        return new RowFilter<>() {
            public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                Object raw = entry.getValue(col);
                String cell = raw == null ? "" : raw.toString().toLowerCase();
                return include == set.contains(cell);
            }
        };
    }

    private int getColumnIndex(String columnName) {
        for (int i = 0; i < model.getColumnCount(); i++) {
            if (model.getColumnName(i).equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        return -1;
    }
}