package com.bigphil.parquetviewer;

import com.intellij.icons.AllIcons;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.SystemInfo;
import com.intellij.ui.JBColor;
import com.intellij.ui.components.JBLabel;
import com.intellij.ui.components.JBTabbedPane;
import com.intellij.util.ui.JBUI;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.FlowLayout;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Tool-window root: empty state, recent files and independent closable file tabs. */
public final class ParquetViewerToolWindowPanel implements Disposable {
    private static final String EMPTY_CARD = "empty";
    private static final String TABS_CARD = "tabs";

    private final Project project;
    private final JPanel overlayRoot = new JPanel();
    private final JPanel contentRoot = new JPanel();
    private final CardLayout cardLayout = new CardLayout();
    private final JBTabbedPane fileTabs = new JBTabbedPane();
    private final Map<String, ParquetSessionPanel> sessions = new LinkedHashMap<>();
    private final RecentParquetFilesManager recentFiles;
    private JPanel emptyStatePanel;

    public ParquetViewerToolWindowPanel(Project project) {
        this.project = project;
        this.recentFiles = new RecentParquetFilesManager(project);

        overlayRoot.setLayout(new javax.swing.OverlayLayout(overlayRoot));
        contentRoot.setLayout(cardLayout);
        emptyStatePanel = createEmptyState();
        contentRoot.add(emptyStatePanel, EMPTY_CARD);
        contentRoot.add(fileTabs, TABS_CARD);
        overlayRoot.add(contentRoot);
        ParquetDropSupport.install(overlayRoot, this::openFile);
        showCorrectCard();
    }

    public JPanel component() { return overlayRoot; }

    public void openFile(File file) {
        if (file == null || !file.isFile()) return;
        if (!file.getName().toLowerCase(Locale.ROOT).endsWith(".parquet")) return;

        String key = canonicalPath(file);
        ParquetSessionPanel existing = sessions.get(key);
        if (existing != null) {
            fileTabs.setSelectedComponent(existing.component());
            return;
        }

        ParquetSessionPanel session = new ParquetSessionPanel(project, file, this::openFile);
        sessions.put(key, session);
        fileTabs.addTab(file.getName(), AllIcons.FileTypes.Any_type, session.component(), file.getAbsolutePath());
        int index = fileTabs.indexOfComponent(session.component());
        fileTabs.setTabComponentAt(index, createTabHeader(file, session, key));
        fileTabs.setSelectedIndex(index);
        recentFiles.add(file);
        showCorrectCard();
    }

    @Override
    public void dispose() {
        sessions.values().forEach(ParquetSessionPanel::dispose);
        sessions.clear();
        project.putUserData(ParquetViewerToolWindowFactory.PANEL_KEY, null);
    }

    private JPanel createEmptyState() {
        JPanel outer = new JPanel(new java.awt.GridBagLayout());
        JPanel content = new JPanel();
        content.setLayout(new BoxLayout(content, BoxLayout.Y_AXIS));

        JBLabel title = new JBLabel("No Parquet file opened", SwingConstants.CENTER);
        title.setFont(title.getFont().deriveFont(java.awt.Font.BOLD, title.getFont().getSize2D() + 3f));
        title.setAlignmentX(Component.CENTER_ALIGNMENT);
        JBLabel description = new JBLabel("Open a local .parquet file or drop one anywhere in this window.", SwingConstants.CENTER);
        description.setForeground(JBColor.GRAY);
        description.setAlignmentX(Component.CENTER_ALIGNMENT);
        JButton open = new JButton("Open Parquet File…", AllIcons.Actions.MenuOpen);
        open.setAlignmentX(Component.CENTER_ALIGNMENT);
        open.addActionListener(event -> chooseFile());

        content.add(title);
        content.add(Box.createVerticalStrut(JBUI.scale(8)));
        content.add(description);
        content.add(Box.createVerticalStrut(JBUI.scale(16)));
        content.add(open);

        java.util.List<File> recent = recentFiles.files();
        if (!recent.isEmpty()) {
            content.add(Box.createVerticalStrut(JBUI.scale(24)));
            JBLabel recentTitle = new JBLabel("Recent files");
            recentTitle.setForeground(JBColor.GRAY);
            recentTitle.setAlignmentX(Component.CENTER_ALIGNMENT);
            content.add(recentTitle);
            for (File file : recent.stream().limit(5).toList()) {
                JButton recentButton = new JButton(file.getName());
                recentButton.setBorderPainted(false);
                recentButton.setContentAreaFilled(false);
                recentButton.setToolTipText(file.getAbsolutePath());
                recentButton.setAlignmentX(Component.CENTER_ALIGNMENT);
                recentButton.addActionListener(event -> openFile(file));
                content.add(recentButton);
            }
        }
        outer.add(content);
        return outer;
    }

    private JPanel createTabHeader(File file, ParquetSessionPanel session, String key) {
        JPanel tab = new JPanel(new FlowLayout(FlowLayout.LEFT, JBUI.scale(4), 0));
        tab.setOpaque(false);
        JBLabel label = new JBLabel(file.getName());
        label.setToolTipText(file.getAbsolutePath());
        JButton close = new JButton(AllIcons.Actions.Close);
        close.setBorder(BorderFactory.createEmptyBorder());
        close.setContentAreaFilled(false);
        close.setFocusable(false);
        close.setToolTipText("Close " + file.getName());
        close.addActionListener(event -> closeSession(session, key));
        tab.add(label);
        tab.add(close);
        return tab;
    }

    private void closeSession(ParquetSessionPanel session, String key) {
        session.dispose();
        sessions.remove(key);
        fileTabs.remove(session.component());
        showCorrectCard();
    }

    private void chooseFile() {
        File selected = ParquetFileChooser.choose(project, null);
        if (selected != null) openFile(selected);
    }

    private void showCorrectCard() {
        if (sessions.isEmpty()) {
            contentRoot.remove(emptyStatePanel);
            emptyStatePanel = createEmptyState();
            contentRoot.add(emptyStatePanel, EMPTY_CARD);
            contentRoot.revalidate();
            contentRoot.repaint();
        }
        cardLayout.show(contentRoot, sessions.isEmpty() ? EMPTY_CARD : TABS_CARD);
    }

    private static String canonicalPath(File file) {
        try {
            String path = file.getCanonicalPath();
            return SystemInfo.isFileSystemCaseSensitive ? path : path.toLowerCase(Locale.ROOT);
        } catch (IOException ignored) {
            String path = file.getAbsolutePath();
            return SystemInfo.isFileSystemCaseSensitive ? path : path.toLowerCase(Locale.ROOT);
        }
    }
}
