package com.briankinney.esql.query;

import com.briankinney.esql.esqlParser;

/**
 * Create an object to contain a value represented in the query as a literal value, eg:
 * 'hello' -> String("hello")
 * 12 -> int(12)
 * 3.14 -> float(3.14)
 */
public class LiteralHelper {

    public static Object getLiteral(esqlParser.LiteralContext literalContext) {
        String body = literalContext.getText();
        switch (literalContext.getRuleIndex()) {
            case 0:
                // String literal
                // Remove first and last '
                return body.substring(1, body.length() - 1);
            case 1:
                // Integer literal
                return Integer.parseInt(body);
            case 2:
                // Numeric literal
                // Note: elasticsearch doesn't seem to support decimal
                return Float.parseFloat(body);
            default:
                // TODO: better error handling
                return null;
        }
    }
}
