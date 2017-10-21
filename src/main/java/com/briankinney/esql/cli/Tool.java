package com.briankinney.esql.cli;

import com.briankinney.esql.QueryBuilderListener;
import com.briankinney.esql.esqlLexer;
import com.briankinney.esql.esqlParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Tool {
    public static void main(String[] args) {
        // InputStream inputStream = new ByteArrayInputStream("SELECT _status.actions.trigger_alert.ack.state FROM .watches LIMIT 10;".getBytes());
        InputStream inputStream = null;
        if (args.length == 0 || args[0].equals("-")) {
            inputStream = System.in;
        } else {
            try {
                inputStream = new FileInputStream(args[0]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        CharStream charStream = null;
        try {
            charStream = CharStreams.fromStream(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        TokenSource tokenSource = new esqlLexer(charStream);
        TokenStream tokenStream = new CommonTokenStream(tokenSource);
        esqlParser parser = new esqlParser(tokenStream);
        esqlParser.Search_queryContext searchQuery = parser.search_query();


        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByAddress("localhost", new byte[]{0, 0, 0, 0});
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        // TODO: settings
        TransportClient transportClient = new PreBuiltTransportClient(Settings.builder()
                .put("cluster.name", "docker-cluster").build())
                .addTransportAddress(new InetSocketTransportAddress(esAddress, 9300));

        QueryBuilderListener listener = new QueryBuilderListener(transportClient);
        ParseTreeWalker.DEFAULT.walk(listener, searchQuery);

        SearchRequestBuilder b = listener.getSearchRequestBuilder();

        System.err.println(b.toString());

        ActionFuture<SearchResponse> actionFuture = b.execute();
        SearchResponse response = actionFuture.actionGet();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
