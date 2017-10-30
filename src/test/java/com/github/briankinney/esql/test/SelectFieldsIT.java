package com.github.briankinney.esql.test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SelectFieldsIT extends EsqlTestCase {

    private String messagesIndexName;

    @Before
    public void before() {
        messagesIndexName = randomIndexName("messages");
        createMessagesIndex(messagesIndexName);
    }

    @After
    public void after() {
        deleteIndex(messagesIndexName);
    }

    @Test
    public void TestSimpleSourceFields() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        String query = String.format("SELECT \"from\", \"to\" FROM %s;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);

        SearchHit hit = searchResponse.getHits().getAt(0);

        Map<String, Object> sourceMap = hit.getSourceAsMap();

        assertTrue(sourceMap.containsKey("from"));
        assertEquals("Alice", sourceMap.get("from"));
        assertTrue(sourceMap.containsKey("to"));
        assertEquals("Bob", sourceMap.get("to"));
        assertFalse(sourceMap.containsKey("title"));
        assertFalse(sourceMap.containsKey("body"));
        assertFalse(sourceMap.containsKey("timestamp"));
    }

    @Test
    public void TestScriptField() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Message", 12L);

        waitForEs();

        String query = String.format("SELECT PAINLESS `doc['timestamp'].getValue() * 2` FROM %s;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        SearchHits hits = searchResponse.getHits();

        assertEquals(1, hits.totalHits);

        SearchHit hit = hits.getAt(0);

        assertTrue(hit.getFields().containsKey("script-field-0"));
        assertEquals(24L, hit.getField("script-field-0").getValue());
    }

    @Test
    public void TestBareAggregation() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "message", 10L);
        addMessage(messagesIndexName, "Bob", "Alice", "Open", "Letter", 12L);

        waitForEs();

        String query = String.format("SELECT count(timestamp) FROM %s;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        Aggregations aggs = searchResponse.getAggregations();

        assertEquals(1, aggs.asList().size());

        assertEquals("value_count", aggs.asList().get(0).getType());

        assertEquals(2, ((ValueCount) aggs.asList().get(0)).getValue());
    }

    @Test
    public void TestSimpleGroupBy() {
        for (int i = 0; i < 10; i++) {
            addMessage(messagesIndexName, "Alice", "Bob", "Spam", "Message", 10L * i);
        }
        for (int i = 0; i < 12; i++) {
            addMessage(messagesIndexName, "Bob", "Alice", "Unsubscribe", "UNSUBSCRIBE", 10L * i + 3L);
        }

        waitForEs();

        String query = String.format("SELECT \"from\", count(timestamp) FROM %s GROUP BY 1;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        Aggregations aggs = searchResponse.getAggregations();

        assertEquals(1, aggs.asList().size());

        assertEquals("sterms", aggs.asList().get(0).getType());

        List<? extends Terms.Bucket> buckets = ((Terms) aggs.asList().get(0)).getBuckets();

        for (Terms.Bucket b : buckets) {
            Aggregation subAggregation = b.getAggregations().asList().get(0);
            assertEquals("value_count", subAggregation.getType());
            if (b.getKeyAsString().equals("Alice")) {
                assertEquals(10, ((ValueCount) subAggregation).getValue());
            } else if (b.getKeyAsString().equals("Bob")) {
                assertEquals(12, ((ValueCount) subAggregation).getValue());
            } else {
                fail(String.format("Unexpected bucket key %s", b.getKeyAsString()));
            }
        }
    }
}
