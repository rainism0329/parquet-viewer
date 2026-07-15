package com.bigphil.parquetviewer;

import com.intellij.icons.AllIcons;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.Messages;
import com.intellij.ui.components.JBLabel;
import com.intellij.ui.components.JBTabbedPane;
import com.intellij.util.concurrency.AppExecutorUtil;
import com.intellij.util.ui.JBUI;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/** One independent Parquet file tab inside the tool window. */
public final class ParquetSessionPanel implements ParquetSessionController.Listener {
    private static final long SMART_MATERIALIZE_CELL_LIMIT = 500_000L;

    private final Project project;
    private final File sourceFile;
    private final JPanel root = new JPanel(new BorderLayout());
    private final ParquetSessionState state = new ParquetSessionState();
    private final ParquetSessionController controller;
    private final DataGridPanel dataPanel;
    private final SchemaViewPanel schemaPanel;
    private final MetadataViewPanel metadataPanel = new MetadataViewPanel();
    private final JBTabbedPane viewTabs = new JBTabbedPane();
    private final JBLabel fileNameLabel = new JBLabel("Opening file…");
    private final JBLabel fileDetailsLabel = new JBLabel();
    private final JProgressBar progressBar = new JProgressBar();
    private final Set<Future<?>> virtualLoads = ConcurrentHashMap.newKeySet();
    private final Consumer<File> openAnotherFile;
    private final javax.swing.Timer fileChangeTimer;
    private long loadedLastModified;
    private long loadedLength;
    private volatile boolean disposed;

    public ParquetSessionPanel(Project project, File file, Consumer<File> openAnotherFile) {
        this.project = project;
        this.sourceFile = file;
        this.openAnotherFile = openAnotherFile;
        this.controller = new ParquetSessionController(project, this);
        this.schemaPanel = new SchemaViewPanel(this::showStatus);
        this.dataPanel = new DataGridPanel(new DataGridPanel.Listener() {
            @Override public void previousPage() { changePage(-1); }
            @Override public void nextPage() { changePage(1); }
            @Override public void pageSizeChanged(int pageSize) { changePageSize(pageSize); }
            @Override public void rowDisplayModeChanged(DataGridPanel.RowDisplayMode mode) { applyRowMode(mode); }
            @Override public void chooseColumns() { showColumnChooser(); }
            @Override public void exportData() { exportCurrentView(); }
            @Override public void showFilterHelp() { FilterHelpDialog.show(root); }
        });

        root.add(createHeader(), BorderLayout.NORTH);
        viewTabs.addTab("Data", dataPanel.component());
        viewTabs.addTab("Schema", schemaPanel.component());
        viewTabs.addTab("Metadata", metadataPanel.component());
        root.add(viewTabs, BorderLayout.CENTER);
        fileChangeTimer = new javax.swing.Timer(3000, event -> checkForExternalChange());
        fileChangeTimer.setRepeats(true);
        fileNameLabel.setText(file.getName());
        fileNameLabel.setToolTipText(file.getAbsolutePath());
        controller.openFile(file, dataPanel.selectedPageSize());
    }

    public JPanel component() { return root; }
    public File file() { return state.file(); }

    public void reload() {
        controller.openFile(sourceFile, state.pageSize());
    }

    public void dispose() {
        disposed = true;
        fileChangeTimer.stop();
        cancelVirtualLoads();
        controller.dispose();
    }

    @Override
    public void onLoading(String message) {
        progressBar.setVisible(true);
        progressBar.setIndeterminate(true);
        dataPanel.setLoading(message);
    }

    @Override
    public void onFileLoaded(ParquetSessionController.FileLoadResult result) {
        if (disposed) return;
        state.setPageSize(result.pageSize());
        state.initialize(result.file(), result.summary(), result.columns());
        fileNameLabel.setText(result.file().getName());
        fileNameLabel.setToolTipText(result.file().getAbsolutePath());
        fileDetailsLabel.setText(String.format(
                "%,d rows · %,d columns · %,d row groups · %s",
                result.summary().rowCount(), result.columns().size(), result.summary().rowGroups().size(),
                humanBytes(result.file().length())
        ));
        loadedLastModified = result.file().lastModified();
        loadedLength = result.file().length();
        if (!fileChangeTimer.isRunning()) fileChangeTimer.start();
        schemaPanel.setSummary(result.summary());
        metadataPanel.setSummary(result.summary());
        dataPanel.setRowDisplayMode(DataGridPanel.RowDisplayMode.PAGED);
        dataPanel.installMaterializedModel(
                new ParquetTableModel(result.summary().schema(), result.columns(), result.firstPage()),
                1, result.pageSize(), result.summary().rowCount(), false
        );
        viewTabs.setSelectedIndex(0);
        finishLoading(String.format("Loaded %,d rows", result.firstPage().size()));
    }

    @Override
    public void onPageLoaded(ParquetSessionController.PageLoadResult result) {
        if (disposed || state.file() == null || !state.file().equals(result.file())) return;
        state.setCurrentPage(result.page());
        dataPanel.installMaterializedModel(
                new ParquetTableModel(result.schema(), result.columns(), result.rows()),
                result.page(), result.pageSize(), state.rowCount(), result.allRowsMaterialized()
        );
        finishLoading(result.allRowsMaterialized()
                ? String.format("Loaded all %,d rows into memory", result.rows().size())
                : String.format("Loaded page %,d", result.page()));
    }

    @Override
    public void onFailure(String operation, Throwable error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        progressBar.setVisible(false);
        dataPanel.setError(operation + " failed: " + safeMessage(cause));
    }

    @Override
    public void onCancelled() {
        progressBar.setVisible(false);
        dataPanel.setMessage("Operation cancelled");
    }

    private JPanel createHeader() {
        JPanel header = new JPanel(new BorderLayout(JBUI.scale(8), 0));
        header.setBorder(JBUI.Borders.empty(6, 8));

        JPanel fileInfo = new JPanel(new FlowLayout(FlowLayout.LEFT, JBUI.scale(8), 0));
        fileNameLabel.setFont(fileNameLabel.getFont().deriveFont(java.awt.Font.BOLD));
        fileDetailsLabel.setForeground(com.intellij.ui.JBColor.GRAY);
        fileInfo.add(fileNameLabel);
        fileInfo.add(fileDetailsLabel);
        header.add(fileInfo, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, JBUI.scale(4), 0));
        progressBar.setPreferredSize(new java.awt.Dimension(JBUI.scale(72), JBUI.scale(6)));
        progressBar.setVisible(false);
        JButton reload = new JButton(AllIcons.Actions.Refresh);
        reload.setToolTipText("Reload file");
        reload.addActionListener(event -> reload());
        JButton open = new JButton("Open Another…", AllIcons.Actions.MenuOpen);
        open.addActionListener(event -> chooseAnotherFile());
        actions.add(progressBar);
        actions.add(reload);
        actions.add(open);
        header.add(actions, BorderLayout.EAST);
        return header;
    }

    private void chooseAnotherFile() {
        File selected = ParquetFileChooser.choose(project, null);
        if (selected != null) openAnotherFile.accept(selected);
    }

    private void changePage(int delta) {
        if (state.schema() == null || state.showAllStrategy() != ParquetSessionState.ShowAllStrategy.OFF) return;
        long pages = Math.max(1, (state.rowCount() + state.pageSize() - 1) / state.pageSize());
        int page = state.currentPage() + delta;
        if (page < 1 || page > pages) return;
        state.setCurrentPage(page);
        loadCurrentPage(false);
    }

    private void changePageSize(int pageSize) {
        state.setPageSize(pageSize);
        if (state.schema() == null) return;
        state.setCurrentPage(1);
        applyRowMode(dataPanel.rowDisplayMode());
    }

    private void showColumnChooser() {
        if (state.schema() == null) return;
        ColumnSelectionDialog dialog = new ColumnSelectionDialog(project, state.allColumns(), state.visibleColumns());
        if (!dialog.showAndGet()) return;
        List<String> selected = dialog.selectedColumns();
        if (selected.equals(state.visibleColumns())) return;
        state.setVisibleColumns(selected);
        state.setCurrentPage(1);
        applyRowMode(dataPanel.rowDisplayMode());
    }

    private void applyRowMode(DataGridPanel.RowDisplayMode mode) {
        if (state.schema() == null || mode == null) return;
        cancelVirtualLoads();
        controller.cancelCurrent();

        switch (mode) {
            case PAGED -> {
                state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.OFF);
                state.setCurrentPage(1);
                loadCurrentPage(false);
            }
            case SHOW_ALL_MEMORY -> {
                state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.MATERIALIZED);
                if (confirmLargeMaterialization()) loadCurrentPage(true);
                else {
                    state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.VIRTUALIZED);
                    dataPanel.setRowDisplayMode(DataGridPanel.RowDisplayMode.SHOW_ALL_VIRTUAL);
                    installVirtualModel();
                }
            }
            case SHOW_ALL_VIRTUAL -> {
                state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.VIRTUALIZED);
                installVirtualModel();
            }
            case SHOW_ALL_SMART -> {
                long cells = safeCellCount();
                if (cells <= SMART_MATERIALIZE_CELL_LIMIT) {
                    state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.MATERIALIZED);
                    loadCurrentPage(true);
                } else {
                    state.setShowAllStrategy(ParquetSessionState.ShowAllStrategy.VIRTUALIZED);
                    installVirtualModel();
                }
            }
        }
    }

    private boolean confirmLargeMaterialization() {
        if (safeCellCount() <= SMART_MATERIALIZE_CELL_LIMIT) return true;
        String message = String.format(
                "This will materialize %,d rows × %,d columns in IDE memory.\n\n" +
                        "Virtual mode provides continuous all-row browsing with bounded memory. Continue anyway?",
                state.rowCount(), state.visibleColumns().size()
        );
        return Messages.showYesNoDialog(project, message, "Load All Rows into Memory",
                "Load into Memory", "Use Virtual Mode", Messages.getWarningIcon()) == Messages.YES;
    }

    private void loadCurrentPage(boolean materializeAll) {
        if (state.file() == null || state.schema() == null || state.visibleColumns().isEmpty()) return;
        controller.loadPage(
                state.file(), state.schema(), state.visibleColumns(), state.currentPage(),
                state.pageSize(), materializeAll
        );
    }

    private void installVirtualModel() {
        if (state.file() == null || state.schema() == null) return;
        List<String> columns = state.visibleColumns();
        int chunkSize = state.pageSize();
        File file = state.file();
        org.apache.parquet.schema.MessageType schema = state.schema();
        VirtualParquetTableModel model = new VirtualParquetTableModel(
                schema, columns, state.rowCount(), chunkSize,
                (page, callback) -> loadVirtualPage(file, schema, page, chunkSize, columns, callback)
        );
        dataPanel.installVirtualModel(model, state.rowCount(), chunkSize);
        finishLoading(state.rowCount() > Integer.MAX_VALUE
                ? "Virtual mode exposes the first 2,147,483,647 rows (JTable limit)"
                : "Show all enabled with bounded-memory virtualization");
    }

    private void loadVirtualPage(File file, org.apache.parquet.schema.MessageType schema,
                                 int page, int chunkSize, List<String> columns,
                                 VirtualParquetTableModel.PageCallback callback) {
        if (disposed) return;
        virtualLoads.removeIf(Future::isDone);
        Future<?> future = AppExecutorUtil.getAppExecutorService().submit(() -> {
            try {
                List<Object[]> rows = ParquetService.readPageData(
                        file, schema, columns, page, chunkSize, false
                );
                if (!disposed) callback.loaded(rows);
            } catch (Throwable error) {
                if (!disposed) {
                    callback.failed(error);
                    ApplicationManager.getApplication().invokeLater(
                            () -> dataPanel.setError("Could not load virtual page " + page + ": " + safeMessage(error))
                    );
                }
            }
        });
        virtualLoads.add(future);
    }

    private void cancelVirtualLoads() {
        dataPanel.disposeVirtualModel();
        virtualLoads.forEach(future -> future.cancel(true));
        virtualLoads.clear();
    }

    private void exportCurrentView() {
        if (state.file() == null) return;
        boolean virtual = dataPanel.table().getModel() instanceof VirtualParquetTableModel;
        ExportOptionsDialog dialog = new ExportOptionsDialog(project, state.visibleColumns().size(), virtual);
        if (!dialog.showAndGet()) return;
        if (virtual && dialog.scope() == ExportOptionsDialog.Scope.CURRENT_VIEW) {
            dataPanel.setError("Current-view export is unavailable in virtual mode; choose Whole file instead");
            return;
        }

        String extension = dialog.format() == ExportOptionsDialog.Format.CSV ? ".csv" : ".json";
        javax.swing.JFileChooser chooser = new javax.swing.JFileChooser(state.file().getParentFile());
        String baseName = state.file().getName().replaceFirst("(?i)\\.parquet$", "");
        chooser.setSelectedFile(new File(state.file().getParentFile(), baseName + extension));
        chooser.setDialogTitle("Export " + dialog.format());
        if (chooser.showSaveDialog(root) != javax.swing.JFileChooser.APPROVE_OPTION) return;
        File destination = chooser.getSelectedFile();
        if (!destination.getName().toLowerCase(Locale.ROOT).endsWith(extension)) {
            destination = new File(destination.getParentFile(), destination.getName() + extension);
        }
        if (destination.exists() && Messages.showYesNoDialog(
                project,
                "The file already exists:\n" + destination.getAbsolutePath() + "\n\nReplace it?",
                "Replace Export File",
                "Replace", "Cancel", Messages.getWarningIcon()) != Messages.YES) {
            return;
        }

        List<Object[]> rows = dialog.scope() == ExportOptionsDialog.Scope.CURRENT_VIEW
                ? snapshotVisibleRows()
                : List.of();
        dataPanel.setLoading("Exporting data…");
        ParquetExportService.export(
                project,
                new ParquetExportService.Request(
                        state.file(), state.schema(), state.visibleColumns(), state.rowCount(), rows,
                        dialog.scope(), dialog.format(), destination
                ),
                dataPanel::setMessage,
                error -> dataPanel.setError("Export failed: " + error)
        );
    }

    private List<Object[]> snapshotVisibleRows() {
        javax.swing.JTable table = dataPanel.table();
        List<Object[]> rows = new java.util.ArrayList<>(table.getRowCount());
        for (int viewRow = 0; viewRow < table.getRowCount(); viewRow++) {
            int modelRow = table.convertRowIndexToModel(viewRow);
            Object[] row = new Object[table.getModel().getColumnCount()];
            for (int column = 0; column < table.getModel().getColumnCount(); column++) {
                row[column] = table.getModel().getValueAt(modelRow, column);
            }
            rows.add(row);
        }
        return rows;
    }

    private long safeCellCount() {
        try {
            return Math.multiplyExact(state.rowCount(), state.visibleColumns().size());
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private void finishLoading(String message) {
        progressBar.setVisible(false);
        progressBar.setIndeterminate(false);
        dataPanel.setMessage(message);
    }

    private void checkForExternalChange() {
        File file = state.file();
        if (file == null || !file.exists()) {
            dataPanel.setError("The source file no longer exists");
            return;
        }
        if (file.lastModified() != loadedLastModified || file.length() != loadedLength) {
            dataPanel.setMessage("Source file changed on disk · Reload to refresh");
        }
    }

    private void showStatus(String message) {
        dataPanel.setMessage(message);
    }

    private static String safeMessage(Throwable error) {
        return error.getMessage() == null || error.getMessage().isBlank()
                ? error.getClass().getSimpleName()
                : error.getMessage();
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int unit = (int) (Math.log(bytes) / Math.log(1024));
        String prefix = "KMGTPE".substring(unit - 1, unit);
        return String.format(Locale.ROOT, "%.1f %sB", bytes / Math.pow(1024, unit), prefix);
    }
}
