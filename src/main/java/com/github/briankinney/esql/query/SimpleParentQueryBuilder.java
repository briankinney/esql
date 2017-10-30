package com.github.briankinney.esql.query;


import org.elasticsearch.index.query.QueryBuilder;

/**
 * Simplified interface for building queries that can accept one or more child queries and transform the combination of
 * their results into one query result.
 */
public interface SimpleParentQueryBuilder extends QueryBuilder {
    void addChild(QueryBuilder queryBuilder);
}
