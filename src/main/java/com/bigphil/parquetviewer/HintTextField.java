package com.bigphil.parquetviewer;

import javax.swing.*;
import java.awt.*;

public class HintTextField extends JTextField {
    private final String hint;

    public HintTextField(String hint) {
        this.hint = hint;
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        if (getText().isEmpty() && !isFocusOwner()) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setFont(getFont().deriveFont(Font.ITALIC));
            g2.setColor(Color.GRAY);
            Insets insets = getInsets();
            FontMetrics fm = g2.getFontMetrics();
            int y = (getHeight() - fm.getHeight()) / 2 + fm.getAscent();
            g2.drawString(hint, insets.left + 4, y);
            g2.dispose();
        }
    }
}