package com.bigphil.parquetviewer;

import com.intellij.openapi.fileChooser.FileChooser;
import com.intellij.openapi.fileChooser.FileChooserDescriptor;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.openapi.vfs.VfsUtilCore;

import java.io.File;

final class ParquetFileChooser {
    private ParquetFileChooser() {}

    static File choose(Project project, VirtualFile initialFile) {
        FileChooserDescriptor descriptor = new FileChooserDescriptor(
                true, false, false, false, false, false
        ).withTitle("Open Parquet File")
                .withDescription("Choose a local .parquet file")
                .withFileFilter(file -> "parquet".equalsIgnoreCase(file.getExtension()));
        VirtualFile selected = FileChooser.chooseFile(descriptor, project, initialFile);
        return selected == null ? null : VfsUtilCore.virtualToIoFile(selected);
    }
}
