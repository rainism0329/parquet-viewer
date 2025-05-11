package com.bigphil.parquetviewer;


import javax.swing.*;
import java.awt.*;

public class DropOverlayPanel extends JPanel {
    private boolean visible = false;

    public DropOverlayPanel() {
        setOpaque(false);
        setVisible(false);
        setLayout(new BorderLayout());

        JLabel label = new JLabel("Drop to open...", SwingConstants.CENTER);
        label.setFont(new Font("SansSerif", Font.BOLD, 28));
        label.setForeground(new Color(100, 100, 100, 160));
        add(label, BorderLayout.CENTER);
    }

    public void showOverlay() {
        visible = true;
        setVisible(true);
        repaint();
    }

    public void hideOverlay() {
        visible = false;
        setVisible(false);
        repaint();
    }

    @Override
    protected void paintComponent(Graphics g) {
        if (!isVisible()) return;

        Graphics2D g2 = (Graphics2D) g.create();

        // Semi-transparent dark background for overlay
        g2.setColor(new Color(30, 30, 30, 180)); // dark gray with alpha
        g2.fillRect(0, 0, getWidth(), getHeight());

        // Dashed light border rectangle
        float[] dash = {6f, 6f};
        g2.setColor(new Color(180, 180, 180, 180)); // light gray dash border
        g2.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, dash, 0));
        g2.drawRect(10, 10, getWidth() - 20, getHeight() - 20);

        g2.dispose();
        super.paintComponent(g);
    }
}