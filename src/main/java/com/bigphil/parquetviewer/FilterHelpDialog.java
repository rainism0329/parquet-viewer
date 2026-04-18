package com.bigphil.parquetviewer;

import com.intellij.ide.BrowserUtil;
import com.intellij.ui.JBColor;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

public class FilterHelpDialog {

    public static void show(Component parent) {
        // --- 1. 构建内容 (加入彩蛋提示) ---
        String helpText = "<html><body style='width: 320px'>"
                + "<h3>Supported Filter Syntax</h3>"
                + "<table cellpadding='4' cellspacing='0'>"
                + "<tr><td><b>name = 'Alice'</b></td><td>String equals</td></tr>"
                + "<tr><td><b>age != 30</b></td><td>Not equal</td></tr>"
                + "<tr><td><b>score &gt;= 80</b></td><td>Numeric comparison</td></tr>"
                + "<tr><td><b>country IN ('US', 'UK')</b></td><td>In list</td></tr>"
                + "<tr><td><b>status NOT IN ('X', 'Y')</b></td><td>Not in list</td></tr>"
                + "<tr><td><b>email IS NULL</b></td><td>Is null</td></tr>"
                + "<tr><td><b>email IS NOT NULL</b></td><td>Not null</td></tr>"
                + "<tr><td><b>name LIKE '%son'</b></td><td>Ends with 'son'</td></tr>"
                + "<tr><td><b>status NOT LIKE '%error%'</b></td><td>Does not contain 'error'</td></tr>"
                + "<tr><td><b>name = 'Alice' AND age &lt; 30</b></td><td>Combine conditions</td></tr>"
                + "<tr><td><b>(region = 'EU' OR region = 'US')</b></td><td>Group with parentheses</td></tr>"
                + "</table>"
                + "<p><b>Notes:</b></p>"
                + "<ul>"
                + "<li>All string comparisons are case-sensitive</li>"
                + "<li>Use single quotes around string values</li>"
                + "<li>Use AND / OR and parentheses to combine expressions</li>"
                + "</ul>"
                // ✨ 核心修改：追加 Easter Egg 隐藏线索
                + "<br><hr style='border: 1px dashed gray;'>"
                + "<p style='color: gray; font-size: 10px; margin-top: 5px;'>"
                + "👾 <b>System Overrides (Easter Eggs):</b><br>"
                + "Type commands directly into the filter bar:<br>"
                + "&nbsp;• <b>rgb</b> - Boost your FPS.<br>"
                + "&nbsp;• <b>matrix</b> (or <b>crt</b>) - Wake up, Neo."
                + "</p>"
                + "</body></html>";

        JLabel htmlLabel = new JLabel(helpText);
        // ✨ 高度从 320 稍微增加到 420，确保彩蛋文字能完全显示
        htmlLabel.setPreferredSize(new Dimension(400, 520));

        // --- 2. 组装主面板 ---
        JPanel mainContent = new JPanel(new BorderLayout(15, 0));
        mainContent.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        mainContent.add(createDonationPanel(), BorderLayout.WEST);
        mainContent.add(htmlLabel, BorderLayout.CENTER);

        // --- 3. 创建自定义 JDialog ---
        // 尝试获取 IDE 的主窗口作为父级，如果获取不到则为 null
        Window windowAncestor = SwingUtilities.getWindowAncestor(parent);
        JDialog dialog = new JDialog(windowAncestor, "Data Filter Help", JDialog.ModalityType.APPLICATION_MODAL);
        dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
        dialog.setResizable(false);

        // 添加底部关闭按钮
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(0, 10, 10, 10));
        JButton closeButton = new JButton("Close");
        closeButton.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        closeButton.addActionListener(e -> dialog.dispose());
        buttonPanel.add(closeButton);

        dialog.add(mainContent, BorderLayout.CENTER);
        dialog.add(buttonPanel, BorderLayout.SOUTH);

        dialog.pack();

        // --- 4. 核心修改：设置位置 ---
        // 如果想让弹窗在 IDEA 窗口正中间：
        dialog.setLocationRelativeTo(windowAncestor);

        dialog.setVisible(true);
    }

    private static JPanel createDonationPanel() {
        ImageIcon qrIcon = new ImageIcon(FilterHelpDialog.class.getResource("/icons/donate3.png"));
        JLabel qrLabel = new JLabel(qrIcon);
        qrLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        JLabel websiteLabel = createLink("Visit Author's Website", "https://phil-the-guy.zeabur.app/");
        JLabel paypalLabel = createLink("Donate via PayPal", "https://www.paypal.me/bigphilzhang");
        JLabel kofiLabel = createLink("Donate via Ko-fi", "https://ko-fi.com/philipzhang51603");

        JLabel supportLabel = new JLabel("Enjoying the plugin?");
        supportLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        supportLabel.setForeground(new Color(0xCC6600));
        supportLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        JPanel donationPanel = new JPanel();
        donationPanel.setLayout(new BoxLayout(donationPanel, BoxLayout.Y_AXIS));
        donationPanel.setBorder(BorderFactory.createEmptyBorder(10, 0, 10, 0)); // 调整内边距
        donationPanel.setAlignmentX(Component.CENTER_ALIGNMENT);

        donationPanel.add(websiteLabel);
        donationPanel.add(Box.createVerticalStrut(8));
        donationPanel.add(qrLabel);
        donationPanel.add(Box.createVerticalStrut(10));
        donationPanel.add(supportLabel);
        donationPanel.add(Box.createVerticalStrut(8));
        donationPanel.add(paypalLabel);
        donationPanel.add(Box.createVerticalStrut(4));
        donationPanel.add(kofiLabel);

        return donationPanel;
    }

    private static JLabel createLink(String text, String url) {
        JLabel label = new JLabel("<html><u>" + text + "</u></html>");
        label.setForeground(JBColor.BLUE);
        label.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        label.setAlignmentX(Component.CENTER_ALIGNMENT);
        label.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                BrowserUtil.browse(url);
            }
        });
        return label;
    }
}