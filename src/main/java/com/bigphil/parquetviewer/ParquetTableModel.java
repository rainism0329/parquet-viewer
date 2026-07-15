package com.bigphil.parquetviewer;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.swing.table.AbstractTableModel;
import java.util.List;

/** Read-only typed model for materialized pages and small Show all results. */
public final class ParquetTableModel extends AbstractTableModel {
    private final MessageType schema;
    private final List<String> columns;
    private final List<Object[]> rows;

    public ParquetTableModel(MessageType schema, List<String> columns, List<Object[]> rows) {
        this.schema = schema;
        this.columns = List.copyOf(columns);
        this.rows = List.copyOf(rows);
    }

    @Override public int getRowCount() { return rows.size(); }
    @Override public int getColumnCount() { return columns.size(); }
    @Override public String getColumnName(int column) { return columns.get(column); }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        Object[] row = rows.get(rowIndex);
        return columnIndex < row.length ? row[columnIndex] : null;
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        try {
            Type type = schema.getType(columns.get(columnIndex));
            if (type.isPrimitive() && !type.isRepetition(Type.Repetition.REPEATED)) {
                PrimitiveType.PrimitiveTypeName name = type.asPrimitiveType().getPrimitiveTypeName();
                return switch (name) {
                    case INT32 -> Integer.class;
                    case INT64 -> Long.class;
                    case FLOAT -> Float.class;
                    case DOUBLE -> Double.class;
                    case BOOLEAN -> Boolean.class;
                    default -> Object.class;
                };
            }
        } catch (RuntimeException ignored) {
        }
        return Object.class;
    }
}
