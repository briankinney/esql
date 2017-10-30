package com.github.briankinney.esql.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

public class NotQueryBuilder extends BoolQueryBuilder implements SimpleParentQueryBuilder {
    @Override
    public void addChild(QueryBuilder queryBuilder) {
        this.mustNot(queryBuilder);
    }
}
