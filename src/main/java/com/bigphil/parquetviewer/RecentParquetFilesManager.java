package com.bigphil.parquetviewer;

import com.intellij.ide.util.PropertiesComponent;
import com.intellij.openapi.project.Project;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class RecentParquetFilesManager {
    private static final String KEY = "parquet-viewer.recentFiles";
    private static final String DELIMITER = "\u001E";
    private static final int MAX_FILES = 10;

    private final PropertiesComponent properties;

    RecentParquetFilesManager(Project project) {
        properties = PropertiesComponent.getInstance(project);
    }

    void add(File file) {
        String path = file.getAbsolutePath();
        List<String> recent = new ArrayList<>(paths());
        recent.remove(path);
        recent.add(0, path);
        if (recent.size() > MAX_FILES) recent = recent.subList(0, MAX_FILES);
        properties.setValue(KEY, String.join(DELIMITER, recent));
    }

    List<File> files() {
        List<File> result = new ArrayList<>();
        for (String path : paths()) {
            File file = new File(path);
            if (file.isFile()) result.add(file);
        }
        return result;
    }

    private List<String> paths() {
        String raw = properties.getValue(KEY);
        if (raw == null || raw.isBlank()) return new ArrayList<>();
        return new ArrayList<>(Arrays.asList(raw.split(DELIMITER, -1)));
    }
}
