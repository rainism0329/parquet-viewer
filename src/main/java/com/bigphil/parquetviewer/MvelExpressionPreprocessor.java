package com.bigphil.parquetviewer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Preprocessor for converting SQL-style expressions to MVEL expressions.
 * <p>
 * This class provides a method to transform an input expression containing SQL-like
 * syntax (e.g., SQL operators and keywords) into an equivalent expression that can
 * be evaluated by the MVEL engine. It supports the following operations (case-insensitive
 * in the input):
 * <ul>
 *   <li><b>LIKE / NOT LIKE:</b> converted to MVEL string comparisons using
 *       {@code contains}, {@code startsWith}, or {@code endsWith}, with case ignored by
 *       converting both the field and pattern to lower-case.</li>
 *   <li><b>IN / NOT IN:</b> converted to a series of equality checks (joined by logical
 *       OR) against the provided list of values. String values are compared in a
 *       case-insensitive manner (by lower-casing), and numeric values are compared directly.</li>
 *   <li><b>IS NULL / IS NOT NULL:</b> converted to null-check expressions (e.g.,
 *       {@code field == null} or {@code field != null}).</li>
 *   <li><b>AND / OR:</b> converted to the logical operators {@code &&} and {@code ||}.</li>
 *   <li><b>= / !=:</b> the equality operator {@code =} is converted to {@code ==} for
 *       MVEL (Java-style equality), and {@code !=} is preserved as the not-equal operator.</li>
 * </ul>
 * <p>
 * All replacements preserve the logical meaning of the original expression. The input
 * keywords are matched in a case-insensitive way (e.g. "AnD" or "and" will be recognized
 * as the AND operator). String comparisons from LIKE and IN operations are performed
 * case-insensitively by converting string operands to lower-case in the generated expression.
 */
public class MvelExpressionPreprocessor {

    /**
     * Converts a SQL-like conditional expression into an equivalent MVEL expression string.
     *
     * @param expr the SQL-style expression to convert (e.g., "name LIKE 'John%' AND age IN (20,30)")
     * @return the transformed expression that follows MVEL syntax, or {@code null} if the input was null
     */
    public static String preprocess(String expr) {
        if (expr == null) {
            return null;
        }

        // 1. Handle "IS NOT NULL" (convert to " != null")
        //    and "IS NULL" (convert to " == null").
        // Use case-insensitive matching for the keywords.
        // We avoid leading spaces in replacements to prevent duplicate spacing.
        expr = expr.replaceAll("(?i)\\bis\\s+not\\s+null\\b", " != null");
        expr = expr.replaceAll("(?i)\\bis\\s+null\\b", " == null");

        // 2. Handle "NOT LIKE" and "LIKE".
        // Convert "field LIKE 'pattern'" to a null-safe MVEL string match expression.
        // We will treat the pattern with SQL wildcards:
        //    '%' in pattern -> wildcard (any sequence of characters)
        //    (Leading and/or trailing '%' determine use of contains/startsWith/endsWith)
        //    (If no '%' at either end, use an equals comparison)
        // For case-insensitivity, both field value and pattern are lower-cased in the comparison.
        Pattern likePattern = Pattern.compile("(?i)\\b([A-Za-z0-9_\\.]+)\\s+like\\s+'([^']*)'");
        Pattern notLikePattern = Pattern.compile("(?i)\\b([A-Za-z0-9_\\.]+)\\s+not\\s+like\\s+'([^']*)'");

        // Replace NOT LIKE first to avoid conflict with LIKE.
        Matcher m = notLikePattern.matcher(expr);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String field = m.group(1);
            String patternStr = m.group(2);
            String likeCondition = buildLikeCondition(field, patternStr, false);
            // Negate the LIKE condition for NOT LIKE.
            String replacement = "!(" + likeCondition + ")";
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        expr = sb.toString();

        // Now handle LIKE
        m = likePattern.matcher(expr);
        sb.setLength(0);  // reuse the buffer
        while (m.find()) {
            String field = m.group(1);
            String patternStr = m.group(2);
            String replacement = buildLikeCondition(field, patternStr, false);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        expr = sb.toString();

        // 3. Handle "NOT IN" and "IN".
        // Convert "field IN (val1, val2, ...)" to a series of comparisons joined by ||.
        // Numeric values will be compared directly (using ==), string literals will be compared
        // case-insensitively (field.toLowerCase().equals("value")).
        Pattern inPattern = Pattern.compile("(?i)\\b([A-Za-z0-9_\\.]+)\\s+in\\s*\\(([^)]*)\\)");
        Pattern notInPattern = Pattern.compile("(?i)\\b([A-Za-z0-9_\\.]+)\\s+not\\s+in\\s*\\(([^)]*)\\)");

        // Replace NOT IN first
        m = notInPattern.matcher(expr);
        sb.setLength(0);
        while (m.find()) {
            String field = m.group(1);
            String valuesStr = m.group(2);
            String inCondition = buildInCondition(field, valuesStr, false);
            // Negate the IN condition for NOT IN.
            String replacement = "!(" + inCondition + ")";
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        expr = sb.toString();

        // Replace IN
        m = inPattern.matcher(expr);
        sb.setLength(0);
        while (m.find()) {
            String field = m.group(1);
            String valuesStr = m.group(2);
            String replacement = buildInCondition(field, valuesStr, false);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        expr = sb.toString();

        // 4. Replace "=" with "==" for equality, ignoring cases where "=" is already part of "==",
        //    "!=" or comparison operators like "<=" or ">=".
        expr = expr.replaceAll("(?i)(?<![<>=!])=(?!=)", "==");

        // 5. Replace logical connectors "AND" / "OR" with "&&" / "||"
        expr = expr.replaceAll("(?i)\\band\\b", "&&");
        expr = expr.replaceAll("(?i)\\bor\\b", "||");

        return expr;
    }

    /**
     * Helper to build a MVEL condition for a SQL LIKE pattern.
     * @param field    the field name (as in the expression) to apply the LIKE to
     * @param pattern  the pattern string (content inside the quotes in the LIKE expression)
     * @param negation whether the condition should be negated (NOT LIKE scenario)
     * @return a string representing the equivalent MVEL condition for the LIKE
     */
    private static String buildLikeCondition(String field, String pattern, boolean negation) {
        // Convert the SQL wildcard pattern to a condition.
        // We handle leading/trailing '%' wildcards.
        String lcPattern = pattern.toLowerCase();  // case-insensitive matching
        String condition;
        if (lcPattern.startsWith("%") && lcPattern.endsWith("%")) {
            // Both leading and trailing wildcard: use contains
            String inner = lcPattern.substring(1, lcPattern.length() - 1);
            // Escape quotes and backslashes in the pattern for safe insertion into the string literal
            String safeInner = inner.replace("\\", "\\\\").replace("\"", "\\\"");
            condition = field + " != null && " + field + ".toLowerCase().contains(\"" + safeInner + "\")";
        } else if (lcPattern.startsWith("%")) {
            // Leading wildcard only: use endsWith
            String inner = lcPattern.substring(1);
            String safeInner = inner.replace("\\", "\\\\").replace("\"", "\\\"");
            condition = field + " != null && " + field + ".toLowerCase().endsWith(\"" + safeInner + "\")";
        } else if (lcPattern.endsWith("%")) {
            // Trailing wildcard only: use startsWith
            String inner = lcPattern.substring(0, lcPattern.length() - 1);
            String safeInner = inner.replace("\\", "\\\\").replace("\"", "\\\"");
            condition = field + " != null && " + field + ".toLowerCase().startsWith(\"" + safeInner + "\")";
        } else {
            // No wildcards: use equals for exact match (case-insensitive)
            String safePattern = lcPattern.replace("\\", "\\\\").replace("\"", "\\\"");
            condition = field + " != null && " + field + ".toLowerCase().equals(\"" + safePattern + "\")";
        }
        if (negation) {
            // For NOT LIKE, negate the entire condition.
            return "!(" + condition + ")";
        }
        return condition;
    }

    /**
     * Helper to build a MVEL condition for a SQL IN list.
     * @param field     the field name to compare
     * @param valuesStr the string inside the parentheses of the IN expression (comma-separated values)
     * @param negation  whether the condition should be negated (NOT IN scenario)
     * @return a string representing the equivalent MVEL condition for the IN list
     */
    private static String buildInCondition(String field, String valuesStr, boolean negation) {
        List<String> values = new ArrayList<>();
        List<Boolean> isString = new ArrayList<>();
        String s = valuesStr.trim();
        int len = s.length();
        StringBuilder token = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (inQuote) {
                if (c == '\\' && i + 1 < len) {
                    token.append(s.charAt(i + 1));
                    i++;
                } else if (c == '\'') {
                    inQuote = false;
                    values.add(token.toString());
                    isString.add(true);
                    token.setLength(0);
                } else {
                    token.append(c);
                }
            } else {
                if (c == '\'') {
                    inQuote = true;
                    // bug fix here, clear the token before "'"
                    token.setLength(0);
                } else if (c == ',') {
                    String val = token.toString().trim();
                    if (!val.isEmpty()) {
                        values.add(val);
                        isString.add(false);
                    }
                    token.setLength(0);
                } else if (c == ')') {
                    String val = token.toString().trim();
                    if (!val.isEmpty()) {
                        values.add(val);
                        isString.add(false);
                    }
                    token.setLength(0);
                } else {
                    token.append(c);
                }
            }
        }
        if (token.length() > 0) {
            String val = token.toString().trim();
            if (!val.isEmpty()) {
                values.add(val);
                isString.add(false);
            }
        }

        if (values.isEmpty()) {
            return negation ? "true" : "false";
        }

        boolean hasStringValue = isString.contains(true);

        StringBuilder condition = new StringBuilder();
        condition.append(field).append(" != null && (");
        for (int idx = 0; idx < values.size(); idx++) {
            String val = values.get(idx);
            boolean valIsString = isString.get(idx);
            if (idx > 0) {
                condition.append(" || ");
            }
            if (hasStringValue) {
                String lowerVal = val.toLowerCase();
                String safeVal = lowerVal.replace("\\", "\\\\").replace("\"", "\\\"");
                condition.append(field).append(".toLowerCase().equals(\"").append(safeVal).append("\")");
            } else {
                condition.append(field).append(" == ").append(val.trim());
            }
        }
        condition.append(")");

        String condStr = condition.toString();
        return negation ? "!(" + condStr + ")" : condStr;
    }
}