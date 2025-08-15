package com.bigphil.parquetviewer;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.util.ArrayList;
import java.util.List;

public class ParquetValueFormatter {

    public static String format(Group group, int fieldIndex) {
        try {
            if (group == null || group.getFieldRepetitionCount(fieldIndex) == 0) {
                return "";
            }

            Type type = group.getType().getType(fieldIndex);

            if (type.isPrimitive()) {
                return cleanValue(group.getValueToString(fieldIndex, 0));
            }

            // Handle repeated field (list)
            if (type.isRepetition(Repetition.REPEATED)) {
                return cleanValue(formatRepeatedField(group, fieldIndex));
            }

            // Handle non-primitive types (struct or map)
            GroupType groupType = type.asGroupType();

            // Detect and process map type
            if (isMapType(groupType)) {
                return cleanValue(formatMapField(group, fieldIndex));
            }

            // Handle regular struct
            return cleanValue(formatStructField(group, fieldIndex));

        } catch (Exception e) {
            return "[Error formatting value]";
        }
    }

    private static String formatRepeatedField(Group group, int fieldIndex) {
        int count = group.getFieldRepetitionCount(fieldIndex);
        List<String> elements = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            try {
                Type subType = group.getType().getType(fieldIndex);

                if (subType.isPrimitive()) {
                    elements.add(group.getValueToString(fieldIndex, i));
                } else {
                    Group subGroup = group.getGroup(fieldIndex, i);

                    // Handle nested list case: extract actual element value
                    if (isListElementType(subGroup.getType())) {
                        elements.add(extractListElementValue(subGroup));
                    } else if (isListType(subGroup.getType())) {
                        // Special handling for list wrapper: extract inner list directly
                        elements.add(extractListValue(subGroup));
                    } else {
                        // Format regular struct normally
                        elements.add(formatGroup(subGroup));
                    }
                }
            } catch (Exception e) {
                elements.add("[error]");
            }
        }

        return "[" + String.join(", ", elements) + "]";
    }

    // Check if type is list element type (struct with single field)
    private static boolean isListElementType(GroupType type) {
        return type.getFieldCount() == 1 &&
                ("element".equals(type.getFieldName(0)) ||
                        "list".equals(type.getFieldName(0)));
    }

    // Check if type is list wrapper type
    private static boolean isListType(GroupType type) {
        return type.getFieldCount() == 1 &&
                type.getType(0).isRepetition(Repetition.REPEATED);
    }

    // Extract actual element value from list wrapper
    private static String extractListElementValue(Group listElement) {
        // Get first (and only) field
        int elementFieldIndex = 0;
        Type elementType = listElement.getType().getType(elementFieldIndex);

        if (elementType.isPrimitive()) {
            return listElement.getValueToString(elementFieldIndex, 0);
        } else {
            // Handle nested list
            return format(listElement, elementFieldIndex);
        }
    }

    // Extract actual list value from list wrapper
    private static String extractListValue(Group listWrapper) {
        // Get first (and only) field (should be the list)
        int listFieldIndex = 0;
        return format(listWrapper, listFieldIndex);
    }

    private static String formatStructField(Group group, int fieldIndex) {
        Group structGroup = group.getGroup(fieldIndex, 0);
        return formatGroup(structGroup);
    }

    private static String formatMapField(Group group, int fieldIndex) {
        Group mapGroup = group.getGroup(fieldIndex, 0);
        int keyValueCount = mapGroup.getFieldRepetitionCount(0);
        List<String> entries = new ArrayList<>();

        for (int i = 0; i < keyValueCount; i++) {
            Group keyValue = mapGroup.getGroup(0, i);
            String key = cleanValue(keyValue.getValueToString(0, 0));
            String value;

            try {
                // Recursively format value
                value = format(keyValue, 1);
            } catch (Exception ex) {
                value = "[error]";
            }

            entries.add(key + ": " + value);
        }

        return "{" + String.join(", ", entries) + "}";
    }

    private static String formatGroup(Group group) {
        List<String> fields = new ArrayList<>();
        GroupType groupType = group.getType();

        for (int i = 0; i < groupType.getFieldCount(); i++) {
            String name = groupType.getFieldName(i);

            if (group.getFieldRepetitionCount(i) > 0) {
                String val = format(group, i);
                fields.add(name + ": " + cleanValue(val));
            }
        }

        return "{" + String.join(", ", fields) + "}";
    }

    // Detect map type
    private static boolean isMapType(GroupType groupType) {
        if (groupType.getFieldCount() != 1) return false;

        Type outerType = groupType.getType(0);
        if (!outerType.isRepetition(Repetition.REPEATED)) return false;

        if (!(outerType instanceof GroupType)) return false;

        GroupType keyValueType = (GroupType) outerType;
        if (keyValueType.getFieldCount() != 2) return false;

        String keyName = keyValueType.getFieldName(0);
        String valueName = keyValueType.getFieldName(1);

        // Support different key/value naming conventions
        return ("key".equals(keyName) && "value".equals(valueName)) ||
                ("key_value".equals(keyName) && valueName != null);
    }

    // Clean value by removing Parquet-specific wrappers
    private static String cleanValue(String value) {
        // Handle list wrapper
        if (value.startsWith("{list: [") && value.endsWith("]}")) {
            String inner = value.substring(8, value.length() - 2);
            return "[" + inner + "]";
        }

        // Handle element wrapper
        if (value.startsWith("{element: ") && value.endsWith("}")) {
            return value.substring(10, value.length() - 1);
        }

        // Handle list element prefix
        if (value.startsWith("list element: ")) {
            return value.substring(14);
        }

        return value;
    }
}