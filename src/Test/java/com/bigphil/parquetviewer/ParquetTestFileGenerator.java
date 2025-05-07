package com.bigphil.parquetviewer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class ParquetTestFileGenerator {
    public static void main(String[] args) throws Exception {
        // 1. 定义 Avro schema（用字符串写最方便）
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"price\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder1\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder2\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder3\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder4\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder5\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder6\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder7\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder8\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder9\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder10\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder11\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder12\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder13\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder14\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder15\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder16\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder17\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder18\", \"type\": \"double\"},\n" +
                "    {\"name\": \"placeholder19\", \"type\": \"double\"}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaJson);

        // 2. 创建 ParquetWriter
        File file = new File("test/test2.parquet");

// 确保目录存在
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

// 如果文件已存在，删除
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException("Failed to delete existing file: " + file.getAbsolutePath());
            }
        }

// 然后创建 ParquetWriter，路径保持一致
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(file.getPath()))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        // 3. 写入数据
        for (int i = 1; i <= 5010; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", i);
            record.put("name", "item-" + i);
            record.put("price", i * 10.5);
            record.put("placeholder1", i * 2);
            record.put("placeholder2", i * 2);
            record.put("placeholder3", i * 2);
            record.put("placeholder4", i * 2);
            record.put("placeholder5", i * 2);
            record.put("placeholder6", i * 2);
            record.put("placeholder7", i * 2);
            record.put("placeholder8", i * 2);
            record.put("placeholder9", i * 2);
            record.put("placeholder10", i * 2);
            record.put("placeholder11", i * 2);
            record.put("placeholder12", i * 2);
            record.put("placeholder13", i * 2);
            record.put("placeholder14", i * 2);
            record.put("placeholder15", i * 2);
            record.put("placeholder16", i * 2);
            record.put("placeholder17", i * 2);
            record.put("placeholder18", i * 2);
            record.put("placeholder19", i * 2);
            writer.write(record);
        }

        writer.close();
        System.out.println("Parquet file 'test.parquet' written successfully.");
    }
}