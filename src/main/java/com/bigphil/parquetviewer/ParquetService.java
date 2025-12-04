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
            // 保持 Step 1 的优化：直接从 Footer 获取总行数，不遍历数据
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

    /**
     * Step 2 核心优化：Row Group Skipping (智能跳块)
     */
    public static List<Object[]> readPageData(File file, MessageType schema, List<String> selectedColumns,
                                              int page, int pageSize, boolean showAll) throws IOException {
        List<Object[]> rows = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            // 1. 计算目标数据的起始行号
            long startRow = showAll ? 0 : (long) (page - 1) * pageSize;
            long maxRows = showAll ? -1 : pageSize;
            long currentRow = 0;

            // 2. 获取所有行组 (Row Groups/Blocks) 的元数据
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();

            for (BlockMetaData block : blocks) {
                long rowsInBlock = block.getRowCount();

                // ---------------------------------------------------------
                // 核心优化 A: 块级跳过 (Row Group Skipping)
                // 如果当前块的所有数据都在目标起始行之前，直接跳过整个块的 IO 和解压
                // ---------------------------------------------------------
                if (currentRow + rowsInBlock <= startRow) {
                    reader.skipNextRowGroup(); // 极速跳过，不消耗 IO
                    currentRow += rowsInBlock;
                    continue;
                }

                // 如果已经收集够了数据（且不是显示全部），则提前结束读取
                if (maxRows != -1 && rows.size() >= maxRows) {
                    break;
                }

                // 3. 只有当块包含我们需要的数据时，才开始读取 (IO + 解压)
                PageReadStore pageStore = reader.readNextRowGroup();
                if (pageStore == null) break;

                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(schema));

                // 4. 块内遍历
                for (long i = 0; i < rowsInBlock; i++) {
                    // 核心优化 B 的修正: 块内行跳过
                    // 修正：RecordReader 没有 skip() 方法，必须调用 read() 来消耗数据
                    if (currentRow < startRow) {
                        recordReader.read(); // 虽然会构建对象，但比全量加载要好，且仅限于目标块内部
                        currentRow++;
                        continue;
                    }

                    // 再次检查是否读满当前页
                    if (maxRows != -1 && rows.size() >= maxRows) {
                        break;
                    }

                    // 真正的读取和格式化逻辑
                    Group group = recordReader.read();
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
                    currentRow++;
                }
            }
        }
        return rows;
    }
}