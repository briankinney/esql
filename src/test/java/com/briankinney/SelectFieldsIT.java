package com.briankinney;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class SelectFieldsIT extends EsqlTestCase {

    private String indexName;

    @Before
    public void before() {
        indexName = randomIndexName();
        createMessagesIndex(indexName);
    }

    @After
    public void after() {
        deleteIndex(indexName);
    }

    @Test
    public void TestSimpleSourceFields() {
        addMessage(indexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        String query = String.format("SELECT from, to FROM %s;", indexName);

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
}
