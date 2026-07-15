package com.bigphil.parquetviewer;

import com.intellij.ui.JBColor;
import com.intellij.ui.components.JBLabel;
import com.intellij.util.ui.JBUI;
import com.intellij.util.ui.UIUtil;

import javax.swing.JPanel;
import javax.swing.SwingConstants;
import java.awt.*;

public class DropOverlayPanel extends JPanel {
    public DropOverlayPanel() {
        setOpaque(false);
        setVisible(false);
        setLayout(new BorderLayout());

        JBLabel label = new JBLabel("Drop Parquet files to open", SwingConstants.CENTER);
        label.setFont(label.getFont().deriveFont(Font.BOLD, label.getFont().getSize2D() + 8f));
        label.setForeground(JBColor.foreground());
        add(label, BorderLayout.CENTER);
    }

    public void showOverlay() {
        setVisible(true);
        repaint();
    }

    public void hideOverlay() {
        setVisible(false);
        repaint();
    }

    @Override
    protected void paintComponent(Graphics g) {
        if (!isVisible()) return;

        Graphics2D g2 = (Graphics2D) g.create();

        Color background = UIUtil.getPanelBackground();
        g2.setColor(new Color(background.getRed(), background.getGreen(), background.getBlue(), 224));
        g2.fillRect(0, 0, getWidth(), getHeight());

        float[] dash = {6f, 6f};
        Color border = JBColor.border();
        g2.setColor(new Color(border.getRed(), border.getGreen(), border.getBlue(), 220));
        g2.setStroke(new BasicStroke(JBUI.scale(2), BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, dash, 0));
        int inset = JBUI.scale(12);
        g2.drawRoundRect(inset, inset, getWidth() - inset * 2, getHeight() - inset * 2,
                JBUI.scale(12), JBUI.scale(12));

        g2.dispose();
        super.paintComponent(g);
    }
}
