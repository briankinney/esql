package com.briankinney.esql;


import com.briankinney.esql.query.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class QueryBuilderListener extends esqlBaseListener {

    private TransportClient transportClient;

    private String method;

    private String endpoint;

    private SearchRequestBuilder searchRequestBuilder;

    private QueryBuilder filterQueryBuilder;

    public QueryBuilderListener() {
        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            System.out.println("Unknown Host!!!!");
            return;
        }
        this.transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(esAddress, 9300));
    }

    public void enterSearch_query(esqlParser.Search_queryContext ctx) {
        this.searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(this.transportClient);
    }

    public void exitSearch_query(esqlParser.Search_queryContext ctx) {
        ActionFuture<SearchResponse> actionFuture = this.searchRequestBuilder.execute();
        SearchResponse response = actionFuture.actionGet();
        for (int i = 0; i < response.getHits().totalHits; i++) {
            StreamOutput out = new OutputStreamStreamOutput(System.out);
            try {
                response.writeTo(out);
            } catch (IOException e) {
                System.err.println("Unable to write query results!!!");
            }
        }
    }

    private boolean gatherPaths = false;

    public void enterPath_spec(esqlParser.Path_specContext ctx) {
        // If "*", no action is required because default behavior is to return everything
        if (!ctx.getText().equals("*")) {
            this.gatherPaths = true;
        }
    }

    public void exitPath_spec(esqlParser.Path_specContext ctx) {
        // Stop adding fields to the list of stored fields
        this.gatherPaths = false;
    }

    public void enterField(esqlParser.FieldContext ctx) {
        if (this.gatherPaths) {
            // Building the search request
            this.searchRequestBuilder.addStoredField(ctx.getText());
        }
    }

    public void enterIndex_spec(esqlParser.Index_specContext ctx) {
        String indexName = ctx.getText();
        this.searchRequestBuilder.setIndices(indexName);
    }

    private Map<ParserRuleContext, PrimitiveQueryBuilder> queryNodes = new HashMap<ParserRuleContext, PrimitiveQueryBuilder>();

    public void enterFilter_spec(esqlParser.Filter_specContext ctx) {
        if (ctx.getRuleIndex() == 0) {
            // Leaf query
            // get the comparator
            String comparator = ctx.children.get(1).getText();
            // get the field name
            String fieldName = ctx.getChild(esqlParser.FieldContext.class, 0).getText();
            // get the literal
            Object literal = LiteralHelper.getLiteral(ctx.getChild(esqlParser.LiteralContext.class, 0));
            QueryBuilder b = TermQueryHelper.getTermQuery(fieldName, comparator, literal);
            if (this.queryNodes.containsKey(ctx.getParent())) {
                queryNodes.get(ctx.getParent()).addChild(b);
            } else {
                // Assume that this leaf query is the top-level query
                this.searchRequestBuilder.setQuery(b);
            }
        } else if (ctx.getRuleIndex() == 1) {
            // NOT query
            PrimitiveQueryBuilder b = new NotQueryBuilder();
            this.queryNodes.put(ctx, b);
        } else if (ctx.getRuleIndex() == 2) {
            // Parentheses
            if (this.queryNodes.containsKey(ctx.getParent())) {
                // No need to create a new node, just add a new reference to the parent
                this.queryNodes.put(ctx, this.queryNodes.get(ctx.getParent()));
                // If this is not the case, then the top level of the query should be parentheses, so no action is needed
            }
        } else if (ctx.getRuleIndex() == 3) {
            // AND query
            PrimitiveQueryBuilder b = new AndQueryBuilder();
            this.queryNodes.put(ctx, b);
        } else if (ctx.getRuleIndex() == 4) {
            // OR query
            PrimitiveQueryBuilder b = new OrQueryBuilder();
            this.queryNodes.put(ctx, b);
        }
    }
}
