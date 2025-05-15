package com.bigphil.parquetviewer;

import org.junit.jupiter.api.Test;
import javax.swing.RowFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.*;

public class DataFilterParserTest {

    private TableModel createTestModel() {
        String[] columns = {"name", "age", "email"};
        Object[][] data = {
                {"Alice", 30, "alice@example.com"},
                {"Bob", 25, "bob@gmail.com"},
                {"Charlie", 35, ""},
                {"David", null, null},
                {"Eve", 40, "eve@corp.com"}
        };
        return new DefaultTableModel(data, columns);
    }

    private boolean matches(RowFilter<TableModel, Integer> filter, TableModel model, int row) {
        return filter == null || filter.include(new RowFilter.Entry<>() {
            public TableModel getModel() { return model; }
            public int getValueCount() { return model.getColumnCount(); }
            public Object getValue(int index) { return model.getValueAt(row, index); }
            public String getStringValue(int index) {
                Object val = getValue(index);
                return val == null ? "" : val.toString();
            }
            public Integer getIdentifier() { return row; }
        });
    }

    @Test
    public void testEqualsFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("name='Alice'");
        assertTrue(matches(filter, model, 0));
        assertFalse(matches(filter, model, 1));
    }

    @Test
    public void testNotEqualsFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("name!='Alice'");
        assertFalse(matches(filter, model, 0));
        assertTrue(matches(filter, model, 1));
    }

    @Test
    public void testIsNullFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("email IS NULL");
        assertTrue(matches(filter, model, 3));
    }

    @Test
    public void testIsNotNullFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("email IS NOT NULL");
        assertTrue(matches(filter, model, 0));
        assertFalse(matches(filter, model, 3));
    }

    @Test
    public void testGreaterThanFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("age>30");
        assertTrue(matches(filter, model, 2));
        assertTrue(matches(filter, model, 4));
        assertFalse(matches(filter, model, 1));
    }

    @Test
    public void testInFilter() {
        TableModel model = createTestModel();
        RowFilter<TableModel, Integer> filter = new DataFilterParser(model).parse("name IN ('Alice', 'Bob')");
        assertTrue(matches(filter, model, 0));
        assertTrue(matches(filter, model, 1));
        assertFalse(matches(filter, model, 2));
    }
}
