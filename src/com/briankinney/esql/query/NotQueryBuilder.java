package com.briankinney.esql.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

public class NotQueryBuilder extends BoolQueryBuilder implements PrimitiveQueryBuilder {

    @Override
    public void addChild(PrimitiveQueryBuilder primitiveQueryBuilder) {
        this.mustNot(primitiveQueryBuilder);
    }

    @Override
    public void addChild(QueryBuilder queryBuilder) {
        this.mustNot(queryBuilder);
    }
}
