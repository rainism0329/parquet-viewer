package com.bigphil.parquetviewer;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
        appendJavaFields(sb, schema, "    ");
        sb.append("}\n");
        return sb.toString();
    }

    private static void appendJavaFields(StringBuilder sb, GroupType type, String indent) {
        for (int i = 0; i < type.getFieldCount(); i++) {
            Type field = type.getType(i);
            String javaType;
            if (field.isPrimitive()) {
                javaType = convertPrimitiveToJava(field);
            } else {
                javaType = capitalize(field.getName());
            }
            sb.append(indent).append("private ").append(javaType).append(" ").append(field.getName()).append(";\n");
        }

        sb.append("\n").append(indent).append("// Getters and Setters omitted\n");

        for (int i = 0; i < type.getFieldCount(); i++) {
            Type field = type.getType(i);
            if (!field.isPrimitive()) {
                GroupType groupType = field.asGroupType();
                String nestedClassName = capitalize(field.getName());
                sb.append("\n");
                sb.append(indent).append("public static class ").append(nestedClassName).append(" {\n");
                appendJavaFields(sb, groupType, indent + "    ");
                sb.append(indent).append("}\n");
            }
        }
    }

    private static String convertTypeToHive(Type type) {
        if (!type.isPrimitive()) {
            GroupType groupType = type.asGroupType();
            StringBuilder sb = new StringBuilder("STRUCT<");
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                Type field = groupType.getType(i);
                sb.append("`").append(field.getName()).append("`: ").append(convertTypeToHive(field));
                if (i < groupType.getFieldCount() - 1) sb.append(", ");
            }
            sb.append(">");
            return sb.toString();
        }
        return switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> "INT";
            case INT64 -> "BIGINT";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
            case BINARY -> type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                    ? "STRING" : "BINARY";
            case INT96 -> "TIMESTAMP";
            default -> "STRING";
        };
    }

    private static String convertPrimitiveToJava(Type type) {
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

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
