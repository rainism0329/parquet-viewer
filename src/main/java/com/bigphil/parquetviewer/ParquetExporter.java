package com.bigphil.parquetviewer;

import javax.swing.*;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class ParquetExporter {

    public static void exportCsv(Component parent, JTable table, File defaultFile) {
        File file = chooseFile(parent, defaultFile, ".csv");
        if (file == null) return;

        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            TableModel model = table.getModel();

            // Header
            for (int col = 0; col < model.getColumnCount(); col++) {
                pw.print(escapeForCsv(model.getColumnName(col)));
                if (col < model.getColumnCount() - 1) pw.print(",");
            }
            pw.println();

            // Rows (filtered)
            int rowCount = table.getRowCount();
            for (int viewRow = 0; viewRow < rowCount; viewRow++) {
                int modelRow = table.convertRowIndexToModel(viewRow);
                for (int col = 0; col < model.getColumnCount(); col++) {
                    Object val = model.getValueAt(modelRow, col);
                    pw.print(escapeForCsv(val));
                    if (col < model.getColumnCount() - 1) pw.print(",");
                }
                pw.println();
            }
            JOptionPane.showMessageDialog(parent, "CSV exported to: " + file.getAbsolutePath());
        } catch (IOException e) {
            JOptionPane.showMessageDialog(parent, "Failed to export CSV: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void exportJson(Component parent, JTable table, File defaultFile) {
        File file = chooseFile(parent, defaultFile, ".json");
        if (file == null) return;

        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            TableModel model = table.getModel();
            int rowCount = table.getRowCount();

            pw.println("[");
            for (int viewRow = 0; viewRow < rowCount; viewRow++) {
                int modelRow = table.convertRowIndexToModel(viewRow);
                pw.print("  {");
                for (int col = 0; col < model.getColumnCount(); col++) {
                    String key = jsonEscape(model.getColumnName(col));
                    Object val = model.getValueAt(modelRow, col);
                    String jsonVal = val == null ? "null" : "\"" + jsonEscape(val.toString()) + "\"";
                    pw.print("\"" + key + "\":" + jsonVal);
                    if (col < model.getColumnCount() - 1) pw.print(", ");
                }
                pw.print("}");
                if (viewRow < rowCount - 1) pw.println(",");
                else pw.println();
            }
            pw.println("]");
            JOptionPane.showMessageDialog(parent, "Exported to:\n" + file.getAbsolutePath(), "Success", JOptionPane.INFORMATION_MESSAGE);
        } catch (IOException e) {
            JOptionPane.showMessageDialog(parent, "Failed to export JSON: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private static File chooseFile(Component parent, File currentFile, String extension) {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Export " + extension.toUpperCase());
        if (currentFile != null) {
            String defaultName = currentFile.getName().replaceAll("\\.parquet$", "") + extension;
            fileChooser.setSelectedFile(new File(defaultName));
        }
        if (fileChooser.showSaveDialog(parent) == JFileChooser.APPROVE_OPTION) {
            return fileChooser.getSelectedFile();
        }
        return null;
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
                .replace("\r", "\\r").replace("\t", "\\t").replace("\b", "\\b").replace("\f", "\\f");
    }

    private static String escapeForCsv(Object value) {
        if (value == null) return "";
        String str = value.toString();
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            str = str.replace("\"", "\"\"");
            return "\"" + str + "\"";
        }
        return str;
    }
}