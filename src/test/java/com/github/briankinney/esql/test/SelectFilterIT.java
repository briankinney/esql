package com.github.briankinney.esql.test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

public class SelectFilterIT extends EsqlTestCase {

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
    public void TestSimpleSelectFilter() {
        addMessage(messagesIndexName, "World", "Brian", "Hello", "How are you?", 0L);
        addMessage(messagesIndexName, "Brian", "World", "Hello", "Hello world", 1L);
        addMessage(messagesIndexName, "Brian", "World", "Hello", "Hello world", 2L);
        addMessage(messagesIndexName, "Brian", "World", "Hello", "Hello world", 3L);
        addMessage(messagesIndexName, "Brian", "World", "Hello", "Hello world", 4L);
        addMessage(messagesIndexName, "World", "Brian", "Leave me alone", "Go away", 5L);

        waitForEs();

        String query = String.format("SELECT from, to, title, body, timestamp FROM %s WHERE timestamp > 4;", messagesIndexName);

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
    public void TestAndResolvedBeforeOr() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        // WHERE clause should eval to true
        // If OR is evaluated before AND it will eval to false
        String query = String.format("SELECT * FROM %s WHERE from = 'Not Alice' AND to = 'Bob' OR timestamp = 0;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);
    }

    @Test
    public void TestParenthesesResolvedBeforeAnd() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        // Make sure the document got in there
        String query = String.format("SELECT * FROM %s;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);

        // WHERE clause should eval to false
        // If parentheses were not respected it would eval to true
        query = String.format("SELECT * FROM %s WHERE from = 'Not Alice' AND (to = 'Bob' OR timestamp = 0);", messagesIndexName);

        searchResponse = esqlClient.executeSearch(query);

        assertEquals(0, searchResponse.getHits().totalHits);
    }

    @Test
    public void TestNotResolvedBeforeAnd() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Message", 0L);

        waitForEs();

        String query = String.format("SELECT * FROM %s;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);

        query = String.format("SELECT * FROM %s WHERE NOT from = 'Alice' AND to = 'Charles';", messagesIndexName);

        searchResponse = esqlClient.executeSearch(query);

        assertEquals(0, searchResponse.getHits().totalHits);
    }

    @Ignore("Boolean formula of the form literal COMPARATOR field are not implemented yet")
    @Test
    public void TestInvertedComparison() {
        addMessage(messagesIndexName, "Alice", "Bob", "Secret", "Massage ;)", 10L);

        waitForEs();

        String query = String.format("SELECT * FROM %s WHERE 0 < timestamp;", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);
    }

    @Test
    public void TestMatchQuery() {
        addMessage(messagesIndexName, "Alice", "Bob", "Title", "I will send many messages. They are not for sharing", 10);
        addMessage(messagesIndexName, "Alice", "Bob", "A note", "This does not contain the search terms", 11);

        waitForEs();

        String query = String.format("SELECT * FROM %s WHERE body MATCHES 'sharing messages';", messagesIndexName);

        SearchResponse searchResponse = esqlClient.executeSearch(query);

        assertEquals(1, searchResponse.getHits().totalHits);
    }
}
