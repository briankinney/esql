package com.briankinney;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

public class SelectFilterIT extends EsqlTestCase {

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
    public void TestSimpleSelectFilter() {
        addMessage(indexName, "World", "Brian", "Hello", "How are you?", 0L);
        addMessage(indexName, "Brian", "World", "Hello", "Hello world", 1L);
        addMessage(indexName, "Brian", "World", "Hello", "Hello world", 2L);
        addMessage(indexName, "Brian", "World", "Hello", "Hello world", 3L);
        addMessage(indexName, "Brian", "World", "Hello", "Hello world", 4L);
        addMessage(indexName, "World", "Brian", "Leave me alone", "Go away", 5L);

        waitForEs();

        String query = String.format("SELECT from, to, title, body, timestamp FROM %s WHERE timestamp > 4;", indexName);

        SearchResponse searchResponse = esqlClient.executeSearch(new ByteArrayInputStream(query.getBytes()));

        assertEquals(1, searchResponse.getHits().totalHits);
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertEquals("World", hit.getSourceAsMap().get("from"));
        assertEquals("Brian", hit.getSourceAsMap().get("to"));
        assertEquals("Leave me alone", hit.getSourceAsMap().get("title"));
        assertEquals("Go away", hit.getSourceAsMap().get("body"));
        // Not sure why this comes back as an Integer and not a Long
        assertEquals(5, hit.getSourceAsMap().get("timestamp"));
    }

    @Test
    public void TestAndOrOrder() {
        addMessage(indexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        // WHERE clause should eval to true
        // If OR is evaluated before AND it will eval to false
        String query = String.format("SELECT * FROM %s WHERE from = 'Not Alice' AND to = 'Bob' OR timestamp = 0;", indexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);
    }

    @Test
    public void TestParentheses() {
        addMessage(indexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        // Make sure the document got in there
        String query = String.format("SELECT * FROM %s;", indexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);

        // WHERE clause should eval to false
        // If parentheses had no effet it would eval to true
        query = String.format("SELECT * FROM %s WHERE from = 'Not Alice' AND (to = 'Bob' OR timestamp = 0);", indexName);

        searchResponse = esqlClient.executeSearch(query);

        assertEquals(0, searchResponse.getHits().totalHits);
    }
}
