package com.briankinney.esql.query;

import org.elasticsearch.index.query.*;

/**
 * Produces QueryBuilders from simple atomic boolean formulae, such as:
 * age > 24
 */
public class TermQueryHelper {
    private static final String EQUAL = "=";
    private static final String NOT_EQUAL = "!=";
    private static final String LESS_THAN = "<";
    private static final String GREATER_THAN = ">";
    private static final String LESS_THAN_OR_EQUAL = "<=";
    private static final String GREATER_THAN_OR_EQUAL = ">=";
    // TODO: private static final String LIKE = "~=";


    public static QueryBuilder getTermQuery(String field_name, String comparator, Object literal) {
        QueryBuilder b;
        if (comparator.equals(EQUAL)) {
            b = new TermQueryBuilder(field_name, literal);
        } else if (comparator.equals(NOT_EQUAL)) {
            TermQueryBuilder tqb = new TermQueryBuilder(field_name, literal);
            BoolQueryBuilder bqb = new BoolQueryBuilder();
            b = bqb.mustNot(tqb);
        } else if (comparator.equals(LESS_THAN)) {
            b = new RangeQueryBuilder(field_name).lt(literal);
        } else if (comparator.equals(GREATER_THAN)) {
            b = new RangeQueryBuilder(field_name).gt(literal);
        } else if (comparator.equals(LESS_THAN_OR_EQUAL)) {
            b = new RangeQueryBuilder(field_name).lte(literal);
        } else if (comparator.equals(GREATER_THAN_OR_EQUAL)) {
            b = new RangeQueryBuilder(field_name).gte(literal);
        } else {
            // TODO: better error handling
            b = null;
        }
        return b;
    }
}
