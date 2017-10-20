package com.briankinney.esql;


import com.briankinney.esql.query.*;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Stack;

/**
 * Builds a search query using the elasticsearch TransportClient as the query is parsed
 */
public class QueryBuilderListener extends esqlBaseListener {

    private TransportClient transportClient;

    private SearchRequestBuilder searchRequestBuilder;

    public QueryBuilderListener() {
        // TODO: Factor client construction out of this constructor
        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByAddress("localhost", new byte[]{0, 0, 0, 0});
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        // TODO: settings
        this.transportClient = new PreBuiltTransportClient(Settings.builder()
                //.put("client.transport.sniff", true)
                //.put("xpack.security.user", "transport_client_user:changeme")
                .put("cluster.name", "docker-cluster").build())
                .addTransportAddress(new InetSocketTransportAddress(esAddress, 9300));
    }

    public void enterSearch_query(esqlParser.Search_queryContext ctx) {
        this.searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(this.transportClient);
    }

    public void exitSearch_query(esqlParser.Search_queryContext ctx) {
        System.out.println(this.searchRequestBuilder.toString());
        ActionFuture<SearchResponse> actionFuture = this.searchRequestBuilder.execute();
        SearchResponse response = actionFuture.actionGet();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    private boolean gatherPaths = false;

    private LinkedList<String> sources = new LinkedList<String>();

    public void enterPath_spec(esqlParser.Path_specContext ctx) {
        // If "*", no action is required because default behavior is to return everything
        if (!ctx.getText().equals("*")) {
            this.gatherPaths = true;
        }
    }

    public void exitPath_spec(esqlParser.Path_specContext ctx) {
        // Stop adding fields to the list of stored fields
        if (this.gatherPaths) {
            this.searchRequestBuilder.setFetchSource(this.sources.toArray(new String[this.sources.size()]), null);
        }
        this.gatherPaths = false;
    }

    public void enterField(esqlParser.FieldContext ctx) {
        if (this.gatherPaths) {
            // Building the search request
            this.sources.add(ctx.getText());
        }
    }

    public void enterIndex_spec(esqlParser.Index_specContext ctx) {
        String indexName = ctx.getText();
        this.searchRequestBuilder.setIndices(indexName);
    }

    private Stack<PrimitiveQueryBuilder> queryNodes = new Stack<PrimitiveQueryBuilder>();

    public void enterFilter_spec(esqlParser.Filter_specContext ctx) {
        boolean isTop = this.queryNodes.empty();
        PrimitiveQueryBuilder parent = null;
        if (!isTop) {
            parent = this.queryNodes.peek();
        }
        QueryBuilder b = null;
        if (ctx.leaf_query() != null) {
            // Leaf query
            // get the comparator
            String comparator = ctx.leaf_query().COMPARATOR().getText();
            // get the field name
            String fieldName = ctx.leaf_query().field().getText();
            // get the literal
            Object literal = LiteralHelper.getLiteral(ctx.leaf_query().literal());
            b = TermQueryHelper.getTermQuery(fieldName, comparator, literal);
        } else if (ctx.NOT() != null) {
            // NOT query
            PrimitiveQueryBuilder pb = new NotQueryBuilder();
            this.queryNodes.push(pb);
            b = pb;
        } else if (ctx.AND() != null) {
            // AND query
            PrimitiveQueryBuilder pb = new AndQueryBuilder();
            this.queryNodes.push(pb);
            b = pb;
        } else if (ctx.OR() != null) {
            // OR query
            PrimitiveQueryBuilder pb = new OrQueryBuilder();
            this.queryNodes.push(pb);
            b = pb;
        } else {
            // Parentheses
            return;
        }
        // TODO: better error handling when b == null
        // Add this QueryBuilder as a child of the parent in the tree
        if (!isTop) {
            parent.addChild(b);
        } else {
            // Assume that this query is the top-level query
            this.searchRequestBuilder.setQuery(b);
        }
    }

    public void exitFilter_spec(esqlParser.Filter_specContext ctx) {
        if (ctx.AND() != null || ctx.OR() != null || ctx.NOT() != null) {
            // Neither leaf query nor Parentheses
            if (!this.queryNodes.empty()) {
                // Stop adding children to the compound query on top of the stack
                this.queryNodes.pop();
            }
        }
    }

    private SortBuilder lastSortBuilder;

    public void enterSort_atom(esqlParser.Sort_atomContext ctx) {
        String sortTerm = ctx.getChild(esqlParser.Sort_termContext.class, 0).getText();
        if (sortTerm.equals("SCORE")) {
            this.lastSortBuilder = new ScoreSortBuilder();
        } else {
            this.lastSortBuilder = new FieldSortBuilder(sortTerm);
        }
        this.searchRequestBuilder.addSort(this.lastSortBuilder);
    }

    public void enterSort_direction(esqlParser.Sort_directionContext ctx) {
        if (ctx.getRuleIndex() == 0) {
            // ASC
            this.lastSortBuilder.order(SortOrder.ASC);
        } else {
            // DESC
            this.lastSortBuilder.order(SortOrder.DESC);
        }
    }

    public void enterLimit_spec(esqlParser.Limit_specContext ctx) {
        int limit = Integer.parseInt(ctx.getText());
        this.searchRequestBuilder.setSize(limit);
    }
}
