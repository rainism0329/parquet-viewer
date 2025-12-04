package com.bigphil.parquetviewer;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class SchemaCodeGenerator {

    public static String generateHiveDDL(MessageType schema, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type field = schema.getType(i);
            sb.append("  `").append(field.getName()).append("` ").append(convertTypeToHive(field));
            if (i < schema.getFieldCount() - 1) sb.append(",");
            sb.append("\n");
        }

        sb.append(") STORED AS PARQUET;\n");
        return sb.toString();
    }

    public static String generateJavaPojo(MessageType schema, String className) {
        StringBuilder sb = new StringBuilder();
        sb.append("public class ").append(className).append(" {\n");

        for (Type field : schema.getFields()) {
            String javaType = convertTypeToJava(field);
            String name = field.getName();
            sb.append("    private ").append(javaType).append(" ").append(name).append(";\n");
        }
        sb.append("\n    // Getters and Setters omitted\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static String convertTypeToHive(Type type) {
        if (!type.isPrimitive()) return "STRUCT<...>"; // 简化处理
        return switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> "INT";
            case INT64 -> "BIGINT";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
            case BINARY -> (type.getOriginalType() != null && type.getOriginalType().toString().equals("UTF8")) ? "STRING" : "BINARY";
            case INT96 -> "TIMESTAMP";
            default -> "STRING";
        };
    }

    private static String convertTypeToJava(Type type) {
        if (!type.isPrimitive()) return "Object";
        return switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> "Integer";
            case INT64 -> "Long";
            case FLOAT -> "Float";
            case DOUBLE -> "Double";
            case BOOLEAN -> "Boolean";
            case BINARY -> "String";
            default -> "Object";
        };
    }
}