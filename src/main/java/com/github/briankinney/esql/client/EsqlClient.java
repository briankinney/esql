package com.github.briankinney.esql.client;

import com.github.briankinney.esql.query.QueryBuilderListener;
import com.github.briankinney.esql.esqlLexer;
import com.github.briankinney.esql.esqlParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class EsqlClient {

    private TransportClient transportClient;

    public EsqlClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    public ActionFuture<SearchResponse> executeSearchAsync(InputStream sqlInputStream) {
        // TODO: construct generic execute interface
        CharStream charStream = null;
        try {
            charStream = CharStreams.fromStream(sqlInputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        TokenSource tokenSource = new esqlLexer(charStream);
        TokenStream tokenStream = new CommonTokenStream(tokenSource);
        esqlParser parser = new esqlParser(tokenStream);

        // Search-specific code
        esqlParser.Search_queryContext searchQuery = parser.search_query();

        QueryBuilderListener listener = new QueryBuilderListener(transportClient);
        ParseTreeWalker.DEFAULT.walk(listener, searchQuery);

        SearchRequestBuilder b = listener.getSearchRequestBuilder();

        return b.execute();
    }

    public SearchResponse executeSearch(InputStream sqlInputStream) {
        return this.executeSearchAsync(sqlInputStream).actionGet();
    }

    public SearchResponse executeSearch(String sqlString) {
        return this.executeSearch(new ByteArrayInputStream(sqlString.getBytes()));
    }
}
