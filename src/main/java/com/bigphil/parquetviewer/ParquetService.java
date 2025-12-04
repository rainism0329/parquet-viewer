package com.bigphil.parquetviewer;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParquetService {

    // 升级：增加 extraMeta 字段用于存储元数据信息
    public record ParquetMetadata(MessageType schema, long rowCount, Map<String, String> extraMeta) {}

    public static ParquetMetadata readMetadata(File file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            FileMetaData fmd = reader.getFooter().getFileMetaData();
            MessageType schema = fmd.getSchema();
            long rowCount = reader.getRecordCount();

            // --- 提取元数据信息 ---
            Map<String, String> meta = new LinkedHashMap<>();
            meta.put("File Name", file.getName());
            meta.put("Total Rows", String.format("%,d", rowCount));
            meta.put("Columns", String.valueOf(schema.getFieldCount()));
            meta.put("Created By", fmd.getCreatedBy());

            // 提取用户自定义的 Key-Value 元数据
            Map<String, String> userMeta = fmd.getKeyValueMetaData();
            if (userMeta != null && !userMeta.isEmpty()) {
                userMeta.forEach((k, v) -> meta.put("UserMeta: " + k, v));
            }
            // -----------------------

            return new ParquetMetadata(schema, rowCount, meta);
        }
    }

    public static List<Object[]> readPageData(File file, MessageType schema, List<String> selectedColumns,
                                              int page, int pageSize, boolean showAll) throws IOException {
        // (保持原有代码不变)
        List<Object[]> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            PageReadStore pageStore;
            long skip = (long) (page - 1) * pageSize;
            long read = 0;
            long skipped = 0;

            while ((pageStore = reader.readNextRowGroup()) != null) {
                if (!showAll && read >= pageSize) break;

                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(schema));
                long rowsInGroup = pageStore.getRowCount();

                for (int i = 0; i < rowsInGroup; i++) {
                    Group group = recordReader.read();
                    if (!showAll && skipped < skip) {
                        skipped++;
                        continue;
                    }
                    if (!showAll && read >= pageSize) break;

                    Object[] row = new Object[selectedColumns.size()];
                    for (int j = 0; j < selectedColumns.size(); j++) {
                        try {
                            int fieldIndex = schema.getFieldIndex(selectedColumns.get(j));
                            row[j] = ParquetValueFormatter.format(group, fieldIndex);
                        } catch (Exception e) {
                            row[j] = "";
                        }
                    }
                    rows.add(row);
                    read++;
                }
            }
        }
        return rows;
    }
}