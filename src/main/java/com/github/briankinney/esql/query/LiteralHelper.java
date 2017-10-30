package com.github.briankinney.esql.query;

import com.github.briankinney.esql.esqlParser;

/**
 * Create an object to contain a value represented in the query as a literal value, eg:
 * 'hello' -> String("hello")
 * 12 -> int(12)
 * 3.14 -> float(3.14)
 */
public class LiteralHelper {

    public static Object getLiteral(esqlParser.LiteralContext literalContext) {
        String body = literalContext.getText();
        if (literalContext.STRING_LITERAL() != null) {
            // String literal
            // Remove first and last '
            return body.substring(1, body.length() - 1);
        } else if (literalContext.INTEGER_LITERAL() != null) {
            // Integer literal
            return Integer.parseInt(body);
        } else if (literalContext.NUMERIC_LITERAL() != null) {
            // Numeric literal
            // Note: elasticsearch doesn't seem to support decimal
            return Float.parseFloat(body);
        } else {
            // TODO: better error handling
            return null;
        }
    }
}
