package com.briankinney.esql;


import com.briankinney.esql.query.*;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;

/**
 * Builds a search query using the elasticsearch TransportClient as the query is parsed
 */
public class QueryBuilderListener extends esqlBaseListener {

    private TransportClient transportClient;

    private SearchRequestBuilder searchRequestBuilder;

    public QueryBuilderListener(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    public void enterSearch_query(esqlParser.Search_queryContext ctx) {
        this.searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(this.transportClient);
    }

    private boolean gatherPaths = false;

    private LinkedList<String> sources = new LinkedList<String>();

    private Map<String, Script> scriptFields =
            new HashMap<String, Script>();

    private String anonymousScriptFieldName() {
        return String.format("script-field-%d", scriptFields.size());
    }

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
            for (Map.Entry<String, Script> entry : this.scriptFields.entrySet()) {
                this.searchRequestBuilder.addScriptField(entry.getKey(), entry.getValue());
            }
        }
        this.gatherPaths = false;
    }

    public void enterSelected_formula(esqlParser.Selected_formulaContext ctx) {
        if (!this.gatherPaths) {
            // This shouldn't happen
            System.err.println("Entered selected_formula without entering select_spec");
        } else {
            if (ctx.field() != null) {
                // Simple field select
                this.sources.add(IdentifierHelper.extractIdentifier(ctx.getText()));
            } else if (ctx.painless_script() != null) {
                String name = anonymousScriptFieldName();
                String painlessScriptText = ctx.painless_script().PAINLESS_SCRIPT_BODY().getText();
                // Remove the ` marking the beginning and end of the script
                painlessScriptText = painlessScriptText.substring(1, painlessScriptText.length() - 1);
                // Note: depends on default script language == "painless"
                Script painlessScript = new Script(painlessScriptText);
                this.scriptFields.put(name, painlessScript);
            } else if (ctx.aggregate_formula() != null) {
                // TODO
                throw new NotImplementedException();
            }
        }
    }

    public void enterIndex_spec(esqlParser.Index_specContext ctx) {
        String indexName = ctx.getText();
        this.searchRequestBuilder.setIndices(indexName);
    }

    private Stack<SimpleParentQueryBuilder> queryNodes = new Stack<SimpleParentQueryBuilder>();

    public void enterFilter_spec(esqlParser.Filter_specContext ctx) {
        boolean isTop = this.queryNodes.empty();
        SimpleParentQueryBuilder parent = null;
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
            SimpleParentQueryBuilder pb = new NotQueryBuilder();
            this.queryNodes.push(pb);
            b = pb;
        } else if (ctx.AND() != null) {
            // AND query
            SimpleParentQueryBuilder pb = new AndQueryBuilder();
            this.queryNodes.push(pb);
            b = pb;
        } else if (ctx.OR() != null) {
            // OR query
            SimpleParentQueryBuilder pb = new OrQueryBuilder();
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

    public SearchRequestBuilder getSearchRequestBuilder() {
        return this.searchRequestBuilder;
    }
}
