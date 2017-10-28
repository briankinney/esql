package com.briankinney.esql.query;

import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

public class AggregationHelper {

    public static ValuesSourceAggregationBuilder getAggregationBuilder(String aggregationName, String aggregationType) {
        if (aggregationType.toLowerCase().equals("count")) {
            // TODO: figure out this ValueType idea
            return new ValueCountAggregationBuilder(aggregationName, ValueType.LONG);
        }
        else {
            throw new RuntimeException(String.format("Aggregation %s is not supported", aggregationType));
        }
    }
}
