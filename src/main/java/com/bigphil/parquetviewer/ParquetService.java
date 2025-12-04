package com.bigphil.parquetviewer;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type; // 新增导入

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParquetService {

    public record ParquetMetadata(MessageType schema, long rowCount, Map<String, String> extraMeta) {}

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

    public static List<Object[]> readPageData(File file, MessageType schema, List<String> selectedColumns,
                                              int page, int pageSize, boolean showAll) throws IOException {
        List<Object[]> rows = new ArrayList<>();

        // --- Step 3 核心优化: 构建 Projected Schema (列裁剪) ---
        // 只将用户选中的列放入新的 Schema 中，Parquet Reader 将自动忽略未选中的列数据
        List<Type> projectedFields = new ArrayList<>();
        for (String col : selectedColumns) {
            if (schema.containsField(col)) {
                projectedFields.add(schema.getType(col));
            }
        }
        // 防止空选导致异常（虽然 UI 层通常会拦截）
        if (projectedFields.isEmpty()) {
            return rows;
        }
        MessageType projectedSchema = new MessageType(schema.getName(), projectedFields);
        // ----------------------------------------------------

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            long startRow = showAll ? 0 : (long) (page - 1) * pageSize;
            long maxRows = showAll ? -1 : pageSize;
            long currentRow = 0;

            List<BlockMetaData> blocks = reader.getFooter().getBlocks();

            for (BlockMetaData block : blocks) {
                long rowsInBlock = block.getRowCount();

                // 优化 A: 块级跳过 (Row Group Skipping)
                if (currentRow + rowsInBlock <= startRow) {
                    reader.skipNextRowGroup();
                    currentRow += rowsInBlock;
                    continue;
                }

                if (maxRows != -1 && rows.size() >= maxRows) {
                    break;
                }

                PageReadStore pageStore = reader.readNextRowGroup();
                if (pageStore == null) break;

                // --- 关键点：使用 projectedSchema 初始化读取器 ---
                // 这会告诉底层只加载相关的 Column Chunk，极大减少 IO 和内存
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectedSchema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(projectedSchema));
                // ---------------------------------------------

                for (long i = 0; i < rowsInBlock; i++) {
                    // 优化 B: 块内行跳过
                    if (currentRow < startRow) {
                        recordReader.read(); // 这里现在也非常快，因为只反序列化选中的列
                        currentRow++;
                        continue;
                    }

                    if (maxRows != -1 && rows.size() >= maxRows) {
                        break;
                    }

                    Group group = recordReader.read();
                    Object[] row = new Object[selectedColumns.size()];
                    for (int j = 0; j < selectedColumns.size(); j++) {
                        try {
                            String colName = selectedColumns.get(j);
                            // --- 关键点：必须使用 projectedSchema 获取正确的字段索引 ---
                            // 因为 Group 现在的结构是裁剪过的，索引可能和原 Schema 不同
                            int fieldIndex = projectedSchema.getFieldIndex(colName);
                            row[j] = ParquetValueFormatter.format(group, fieldIndex);
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