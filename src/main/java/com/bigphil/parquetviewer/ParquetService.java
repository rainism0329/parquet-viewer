package com.bigphil.parquetviewer;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType; // 新增导入
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ParquetService {

    public record ParquetMetadata(MessageType schema, long rowCount, Map<String, String> extraMeta) {}

    public static class ColumnDetails {
        public long totalCompressedSize = 0;
        public long totalUncompressedSize = 0;
        public long totalNulls = 0;
        public long totalCount = 0;

        public void add(ColumnChunkMetaData meta) {
            totalCompressedSize += meta.getTotalSize();
            totalUncompressedSize += meta.getTotalUncompressedSize();
            totalCount += meta.getValueCount();
            if (meta.getStatistics() != null) {
                totalNulls += meta.getStatistics().getNumNulls();
            }
        }

        public String getCompressionRatio() {
            if (totalCompressedSize == 0) return "1.0x";
            double ratio = (double) totalUncompressedSize / totalCompressedSize;
            return String.format("%.1fx", ratio);
        }

        public String getNullPercentage() {
            if (totalCount == 0) return "0%";
            double pct = (double) totalNulls / totalCount * 100.0;
            return String.format("%.1f%%", pct);
        }
    }

    public static ParquetMetadata readMetadata(File file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            FileMetaData fmd = reader.getFooter().getFileMetaData();
            MessageType schema = fmd.getSchema();
            long rowCount = reader.getRecordCount();

            Map<String, String> meta = new LinkedHashMap<>();
            meta.put("File Name", file.getName());
            meta.put("Total Rows", String.format("%,d", rowCount));
            meta.put("Columns", String.valueOf(schema.getFieldCount()));
            meta.put("Created By", fmd.getCreatedBy());

            Map<String, String> userMeta = fmd.getKeyValueMetaData();
            if (userMeta != null && !userMeta.isEmpty()) {
                userMeta.forEach((k, v) -> meta.put("UserMeta: " + k, v));
            }

            return new ParquetMetadata(schema, rowCount, meta);
        }
    }

    public static Map<String, ColumnDetails> readColumnStats(File file) throws IOException {
        Map<String, ColumnDetails> statsMap = new HashMap<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            for (BlockMetaData block : blocks) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    String path = column.getPath().toDotString();
                    statsMap.compute(path, (k, v) -> {
                        if (v == null) v = new ColumnDetails();
                        v.add(column);
                        return v;
                    });
                }
            }
        }
        return statsMap;
    }

    public static List<Object[]> readPageData(File file, MessageType schema, List<String> selectedColumns,
                                              int page, int pageSize, boolean showAll) throws IOException {
        List<Object[]> rows = new ArrayList<>();

        // Optimization: Column Projection
        List<Type> projectedFields = new ArrayList<>();
        for (String col : selectedColumns) {
            if (schema.containsField(col)) {
                projectedFields.add(schema.getType(col));
            }
        }
        if (projectedFields.isEmpty()) return rows;
        MessageType projectedSchema = new MessageType(schema.getName(), projectedFields);

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            long startRow = showAll ? 0 : (long) (page - 1) * pageSize;
            long maxRows = showAll ? -1 : pageSize;
            long currentRow = 0;

            List<BlockMetaData> blocks = reader.getFooter().getBlocks();

            for (BlockMetaData block : blocks) {
                long rowsInBlock = block.getRowCount();

                // Optimization: Row Group Skipping
                if (currentRow + rowsInBlock <= startRow) {
                    reader.skipNextRowGroup();
                    currentRow += rowsInBlock;
                    continue;
                }

                if (maxRows != -1 && rows.size() >= maxRows) break;

                PageReadStore pageStore = reader.readNextRowGroup();
                if (pageStore == null) break;

                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectedSchema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(projectedSchema));

                for (long i = 0; i < rowsInBlock; i++) {
                    if (currentRow < startRow) {
                        recordReader.read();
                        currentRow++;
                        continue;
                    }

                    if (maxRows != -1 && rows.size() >= maxRows) break;

                    Group group = recordReader.read();
                    Object[] row = new Object[selectedColumns.size()];
                    for (int j = 0; j < selectedColumns.size(); j++) {
                        try {
                            String colName = selectedColumns.get(j);
                            int fieldIndex = projectedSchema.getFieldIndex(colName);

                            // --- 核心修改：保留原始数字类型 ---
                            if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                                row[j] = null; // 显式 Null
                            } else {
                                Type type = projectedSchema.getType(fieldIndex);
                                // 如果是基础类型且不是重复字段（List），尝试提取原始值
                                if (type.isPrimitive() && !type.isRepetition(Type.Repetition.REPEATED)) {
                                    PrimitiveType pt = type.asPrimitiveType();
                                    switch (pt.getPrimitiveTypeName()) {
                                        case INT32:
                                            row[j] = group.getInteger(fieldIndex, 0);
                                            break;
                                        case INT64:
                                            row[j] = group.getLong(fieldIndex, 0);
                                            break;
                                        case FLOAT:
                                            row[j] = group.getFloat(fieldIndex, 0);
                                            break;
                                        case DOUBLE:
                                            row[j] = group.getDouble(fieldIndex, 0);
                                            break;
                                        case BOOLEAN:
                                            row[j] = group.getBoolean(fieldIndex, 0);
                                            break;
                                        default:
                                            // 其他类型（如 Binary String, Int96 Timestamp）继续用 Formatter
                                            row[j] = ParquetValueFormatter.format(group, fieldIndex);
                                    }
                                } else {
                                    // 复杂类型或 List 继续用 Formatter 转 String
                                    row[j] = ParquetValueFormatter.format(group, fieldIndex);
                                }
                            }
                            // ---------------------------------
                        } catch (Exception e) {
                            row[j] = "";
                        }
                    }
                    rows.add(row);
                    currentRow++;
                }
            }
        }
        return rows;
    }
}