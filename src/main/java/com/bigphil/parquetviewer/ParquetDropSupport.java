package com.bigphil.parquetviewer;

import java.awt.datatransfer.*;
import java.awt.dnd.*;
import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import javax.swing.*;

public class ParquetDropSupport {

    public static void install(JComponent target, Consumer<File> parquetFileHandler) {
        DropOverlayPanel overlay = new DropOverlayPanel();
        overlay.setOpaque(false);
        overlay.setVisible(false);

        if (!(target.getLayout() instanceof OverlayLayout)) {
            target.setLayout(new OverlayLayout(target));
        }

        target.add(overlay);
        target.setComponentZOrder(overlay, 0);

        target.addComponentListener(new java.awt.event.ComponentAdapter() {
            @Override
            public void componentResized(java.awt.event.ComponentEvent e) {
                overlay.setBounds(0, 0, target.getWidth(), target.getHeight());
            }
        });

        new DropTarget(target, new DropTargetListener() {
            @Override
            public void dragEnter(DropTargetDragEvent dtde) {
                if (dtde.isDataFlavorSupported(DataFlavor.javaFileListFlavor)) {
                    overlay.showOverlay();
                    dtde.acceptDrag(DnDConstants.ACTION_COPY);
                } else {
                    dtde.rejectDrag();
                }
            }

            @Override
            public void dragExit(DropTargetEvent dte) {
                overlay.hideOverlay();
            }

            @Override
            public void drop(DropTargetDropEvent dtde) {
                overlay.hideOverlay();
                try {
                    dtde.acceptDrop(DnDConstants.ACTION_COPY);
                    Transferable t = dtde.getTransferable();
                    if (t.isDataFlavorSupported(DataFlavor.javaFileListFlavor)) {
                        @SuppressWarnings("unchecked")
                        List<File> files = (List<File>) t.getTransferData(DataFlavor.javaFileListFlavor);
                        boolean accepted = false;
                        for (File f : files) {
                            if (f.isFile() && f.getName().toLowerCase(Locale.ROOT).endsWith(".parquet")) {
                                accepted = true;
                                SwingUtilities.invokeLater(() -> parquetFileHandler.accept(f));
                            }
                        }
                        if (!accepted) {
                            JOptionPane.showMessageDialog(target,
                                    "Only .parquet files are supported.",
                                    "Unsupported File",
                                    JOptionPane.WARNING_MESSAGE);
                        }
                        dtde.dropComplete(accepted);
                        return;
                    }
                    dtde.dropComplete(false);
                } catch (Exception ex) {
                    dtde.dropComplete(false);
                    JOptionPane.showMessageDialog(target,
                            "Failed to handle dropped file: " + ex.getMessage(),
                            "Error",
                            JOptionPane.ERROR_MESSAGE);
                } finally {
                    overlay.hideOverlay();
                }
            }

            @Override public void dragOver(DropTargetDragEvent dtde) {}
            @Override public void dropActionChanged(DropTargetDragEvent dtde) {}
        });
    }
}
