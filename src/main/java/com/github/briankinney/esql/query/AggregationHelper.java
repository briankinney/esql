package com.github.briankinney.esql.query;

import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

class AggregationHelper {

    /**
     * Produce a builder for an aggregation that computes an aggregate over values from a collection of documents
     *
     * @param aggregationName An alias for the aggregation specified in the query or auto-generated when not specified
     * @param aggregationType Name of the aggregation function as implemented in esql
     * @return A ValuesSourceAggregationBuilder that will construct the correct elasticsearch API class to represent the
     * aggregation
     */
    static ValuesSourceAggregationBuilder getAggregationBuilder(String aggregationName, String aggregationType) {
        String lowerType = aggregationType.toLowerCase();
        if (lowerType.equals("count")) {
            // I think ValueType argument won't matter for ValueCountAggregation but I might be misunderstanding
            return new ValueCountAggregationBuilder(aggregationName, ValueType.LONG);
        } else if (lowerType.equals("sum")) {
            return new SumAggregationBuilder(aggregationName);
        } else if (lowerType.equals("avg")) {
            return new AvgAggregationBuilder(aggregationName);
        } else if (lowerType.equals("min")) {
            return new MinAggregationBuilder(aggregationName);
        } else if (lowerType.equals("max")) {
            return new MaxAggregationBuilder(aggregationName);
        } else {
            throw new RuntimeException(String.format("Aggregation %s is not supported", aggregationType));
        }
    }
}
