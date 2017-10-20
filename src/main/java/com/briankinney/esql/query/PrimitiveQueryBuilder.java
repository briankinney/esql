package com.briankinney.esql.query;


import org.elasticsearch.index.query.QueryBuilder;

public interface PrimitiveQueryBuilder extends QueryBuilder {
    void addChild(PrimitiveQueryBuilder primitiveQueryBuilder);
    void addChild(QueryBuilder queryBuilder);
}
