package com.briankinney.esql.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Wrap a BoolQueryBuilder with a simple interface to build simple AND constructs
 */
public class AndQueryBuilder extends BoolQueryBuilder implements PrimitiveQueryBuilder {
    @Override
    public void addChild(PrimitiveQueryBuilder primitiveQueryBuilder) {
        this.must(primitiveQueryBuilder);
    }

    @Override
    public void addChild(QueryBuilder queryBuilder) {
        this.must(queryBuilder);
    }
}
