package com.briankinney.esql.test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        String query = String.format("SELECT from, to FROM %s;", messagesIndexName);

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
