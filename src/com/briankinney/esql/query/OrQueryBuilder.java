package com.briankinney.esql.query;

import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Wrap a dis max query in a simple interface to build simple OR constructs
 */
public class OrQueryBuilder extends DisMaxQueryBuilder implements PrimitiveQueryBuilder {
    @Override
    public void addChild(PrimitiveQueryBuilder primitiveQueryBuilder) {
        this.add(primitiveQueryBuilder);
    }

    @Override
    public void addChild(QueryBuilder queryBuilder) {
        this.add(queryBuilder);
    }
}
