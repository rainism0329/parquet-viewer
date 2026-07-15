package com.bigphil.parquetviewer;

import com.intellij.ide.util.PropertiesComponent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilterHistoryManager {

    private static final String KEY = "parquet-viewer.filterHistory";
    private static final String DELIMITER = "";
    private static final int MAX_SIZE = 20;

    public static void add(String filterText) {
        if (filterText == null || filterText.isBlank()) return;
        String trimmed = filterText.trim();

        List<String> history = new ArrayList<>(getAll());
        history.remove(trimmed);
        history.add(0, trimmed);
        if (history.size() > MAX_SIZE) {
            history = history.subList(0, MAX_SIZE);
        }

        PropertiesComponent.getInstance().setValue(KEY, String.join(DELIMITER, history));
    }

    public static List<String> getAll() {
        String raw = PropertiesComponent.getInstance().getValue(KEY);
        if (raw == null || raw.isEmpty()) return new ArrayList<>();
        return new ArrayList<>(Arrays.asList(raw.split(DELIMITER)));
    }

    public static void clear() {
        PropertiesComponent.getInstance().setValue(KEY, null);
    }
}
