package com.bigphil.parquetviewer;

import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Disposer;
import com.intellij.openapi.util.Key;
import com.intellij.openapi.wm.ToolWindow;
import com.intellij.openapi.wm.ToolWindowFactory;
import com.intellij.ui.content.Content;
import com.intellij.ui.content.ContentFactory;
import org.jetbrains.annotations.NotNull;

public class ParquetViewerToolWindowFactory implements ToolWindowFactory {
    public static final String TOOL_WINDOW_ID = "Parquet Viewer";
    public static final Key<ParquetViewerToolWindowPanel> PANEL_KEY = Key.create("parquet.viewer.toolWindow.panel");

    @Override
    public void createToolWindowContent(@NotNull Project project, @NotNull ToolWindow toolWindow) {
        ParquetViewerToolWindowPanel panel = new ParquetViewerToolWindowPanel(project);
        project.putUserData(PANEL_KEY, panel);

        ContentFactory contentFactory = ContentFactory.getInstance();

        Content content = contentFactory.createContent(panel.component(), "", false);
        Disposer.register(content, panel);
        toolWindow.getContentManager().addContent(content);
    }
}
