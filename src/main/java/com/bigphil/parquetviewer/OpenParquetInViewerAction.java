package com.bigphil.parquetviewer;

import com.intellij.openapi.actionSystem.ActionUpdateThread;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.CommonDataKeys;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.openapi.vfs.VfsUtilCore;
import com.intellij.openapi.wm.ToolWindow;
import com.intellij.openapi.wm.ToolWindowManager;
import org.jetbrains.annotations.NotNull;

/** Project-view context action; the file still opens inside the tool window, never the editor. */
public final class OpenParquetInViewerAction extends AnAction {
    @Override
    public void update(@NotNull AnActionEvent event) {
        VirtualFile file = event.getData(CommonDataKeys.VIRTUAL_FILE);
        boolean parquet = file != null && !file.isDirectory()
                && "parquet".equalsIgnoreCase(file.getExtension())
                && file.isInLocalFileSystem();
        event.getPresentation().setEnabledAndVisible(parquet && event.getProject() != null);
    }

    @Override
    public void actionPerformed(@NotNull AnActionEvent event) {
        Project project = event.getProject();
        VirtualFile file = event.getData(CommonDataKeys.VIRTUAL_FILE);
        if (project == null || file == null) return;
        ToolWindow toolWindow = ToolWindowManager.getInstance(project)
                .getToolWindow(ParquetViewerToolWindowFactory.TOOL_WINDOW_ID);
        if (toolWindow == null) return;
        toolWindow.activate(() -> {
            ParquetViewerToolWindowPanel panel = project.getUserData(ParquetViewerToolWindowFactory.PANEL_KEY);
            if (panel != null) panel.openFile(VfsUtilCore.virtualToIoFile(file));
        });
    }

    @Override
    public @NotNull ActionUpdateThread getActionUpdateThread() {
        return ActionUpdateThread.BGT;
    }
}
