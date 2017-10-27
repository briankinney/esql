package com.briankinney.esql.query;

import com.briankinney.esql.esqlParser;

public class IdentifierHelper {
    /**
     * Convert a token referencing an identifier into a plain string identifier reference.
     *
     * Specifically, if the String begins and ends with ", return the contents of the ". Otherwise return the input
     * String.
     *
     * @param identifierString
     * @return
     */
    public static String extractIdentifier(String identifierString) {
        if (identifierString.startsWith("\"") && identifierString.endsWith("\"")) {
            return identifierString.substring(1, identifierString.length() - 1);
        }
        else {
            return identifierString;
        }
    }
}
