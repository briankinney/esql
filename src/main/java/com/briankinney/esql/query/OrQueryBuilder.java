package com.briankinney.esql.query;

import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Wrap a dis max query in a simple interface to build simple OR constructs
 */
public class OrQueryBuilder extends DisMaxQueryBuilder implements SimpleParentQueryBuilder {
    @Override
    public void addChild(QueryBuilder queryBuilder) {
        this.add(queryBuilder);
    }
}
