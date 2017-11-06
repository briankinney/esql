package com.github.briankinney.esql.query;


import com.github.briankinney.esql.esqlBaseListener;
import com.github.briankinney.esql.esqlParser;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

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

    private boolean hasAggregates = false;

    public void enterSearch_query(esqlParser.Search_queryContext ctx) {
        this.searchRequestBuilder = SearchAction.INSTANCE.newRequestBuilder(this.transportClient);
    }

    public void exitSearch_query(esqlParser.Search_queryContext ctx) {
        // Add aggregations when there are no GROUP BY terms
        if (this.parentAggregationBuilder == null) {
            for (AggregationBuilder ab : this.leafAggregations.values()) {
                this.searchRequestBuilder.addAggregation(ab);
            }
        }

        // Check that all non-aggregate select terms were referenced in the GROUP BY clause
        if (this.hasAggregates && this.nonAggSelectTerms.size() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Object selectTerm : this.nonAggSelectTerms.values()) {
                if (selectTerm instanceof String) {
                    stringBuilder.append((String) selectTerm);
                } else if (selectTerm instanceof Script) {
                    stringBuilder.append(((Script) selectTerm).getIdOrCode());
                }
                stringBuilder.append(',');
            }
            String nonAggNonGroupByList = stringBuilder.toString();

            throw new RuntimeException(String.format("Non aggregate terms: %s", nonAggNonGroupByList));
        }
    }

    private boolean gatherPaths = false;

    private int selectTermIndex = 1;

    private Map<Integer, Object> nonAggSelectTerms = new HashMap<Integer, Object>();

    private LinkedList<String> sources = new LinkedList<String>();

    private Map<String, Script> scriptFields = new HashMap<String, Script>();

    private Map<String, ValuesSourceAggregationBuilder> leafAggregations = new HashMap<String, ValuesSourceAggregationBuilder>();

    private String anonymousScriptFieldName() {
        return String.format("script-field-%d", scriptFields.size());
    }

    private String anonymousAggregationName(String aggregationType) {
        return String.format("%s-aggregation-%d", aggregationType, leafAggregations.size());
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
                String fieldName = IdentifierHelper.extractIdentifier(ctx.getText());
                this.sources.add(fieldName);
                this.nonAggSelectTerms.put(this.selectTermIndex, fieldName);
            } else if (ctx.painless_script() != null) {
                String name = anonymousScriptFieldName();
                String painlessScriptText = ctx.painless_script().PAINLESS_SCRIPT_BODY().getText();
                // Remove the ` marking the beginning and end of the script
                painlessScriptText = painlessScriptText.substring(1, painlessScriptText.length() - 1);
                // Note: depends on default script language == "painless"
                Script painlessScript = new Script(painlessScriptText);
                this.scriptFields.put(name, painlessScript);
                this.nonAggSelectTerms.put(this.selectTermIndex, painlessScript);
            } else if (ctx.aggregate_formula() != null) {
                this.hasAggregates = true;
                String aggregationType = ctx.aggregate_formula().function().getText();
                String aggregationName = anonymousAggregationName(aggregationType);
                ValuesSourceAggregationBuilder aggregationBuilder = AggregationHelper.getAggregationBuilder(aggregationName, aggregationType);
                if (ctx.aggregate_formula().field() != null) {
                    aggregationBuilder.field(ctx.aggregate_formula().field().getText());
                } else if (ctx.aggregate_formula().painless_script() != null) {
                    String scriptText = ctx.aggregate_formula().painless_script().PAINLESS_SCRIPT_BODY().getText();
                    scriptText = scriptText.substring(1, scriptText.length() - 1);
                    Script script = new Script(scriptText);
                    aggregationBuilder.script(script);
                } else {
                    String message = String.format("Unexpected aggregate_formula branch at %s", ctx.getText());
                    throw new RuntimeException(message);
                }
                this.leafAggregations.put(aggregationName, aggregationBuilder);
            }
            this.selectTermIndex++;
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
            esqlParser.Leaf_queryContext leafCtx = ctx.leaf_query();
            if (leafCtx.COMPARATOR() != null) {
                // Leaf query
                // get the comparator
                String comparator = ctx.leaf_query().COMPARATOR().getText();
                // get the field name
                String fieldName = ctx.leaf_query().field().getText();
                // get the literal
                Object literal = LiteralHelper.getLiteral(ctx.leaf_query().literal());
                b = ComparisonQueryHelper.getTermQuery(fieldName, comparator, literal);
            } else if (leafCtx.MATCHES() != null) {
                // Match query
                String fieldName = leafCtx.field().getText();
                String literalText = leafCtx.STRING_LITERAL().getText();
                String terms = literalText.substring(1, literalText.length() - 1);
                b = new MatchQueryBuilder(fieldName, terms);
            }
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
        // TODO: if aggregations are present, sort the results of aggregations
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

    private AggregationBuilder parentAggregationBuilder = null;

    public void enterAggregation_term(esqlParser.Aggregation_termContext ctx) {
        this.hasAggregates = true;
        if (ctx.field_ref() != null) {
            int fieldReference = Integer.parseInt(ctx.getText());
            if (fieldReference <= 0) {
                throw new RuntimeException("Invalid select term reference");
            }
            AggregationBuilder aggregationBuilder = null;
            Object selectedTerm = this.nonAggSelectTerms.get(fieldReference);
            if (selectedTerm instanceof String) {
                // Source field
                String aggregationName = String.format("%s-aggregation", selectedTerm);
                // TODO: figure out this ValueType idea
                TermsAggregationBuilder tab = new TermsAggregationBuilder(aggregationName, ValueType.STRING);
                tab.field((String) selectedTerm);
                aggregationBuilder = tab;
            } else if (selectedTerm instanceof Script) {
                // Script field
                // TODO: Make this name consistent with the other name
                String aggregationName = String.format("script-%s-aggregation", fieldReference);
                TermsAggregationBuilder tab = new TermsAggregationBuilder(aggregationName, ValueType.STRING);
                tab.script((Script) selectedTerm);
                aggregationBuilder = tab;
            }
            if (this.parentAggregationBuilder == null) {
                // Then this is the top-level aggregation
                this.searchRequestBuilder.addAggregation(aggregationBuilder);
                this.parentAggregationBuilder = aggregationBuilder;
            } else {
                this.parentAggregationBuilder.subAggregation(aggregationBuilder);
                this.parentAggregationBuilder = aggregationBuilder;
            }
            this.nonAggSelectTerms.remove(fieldReference);
        } else {
            throw new RuntimeException("Unimplemented GROUP BY term representation");
        }
    }

    public void exitAggregation_spec(esqlParser.Aggregation_specContext ctx) {
        for (AggregationBuilder ab : this.leafAggregations.values()) {
            this.parentAggregationBuilder.subAggregation(ab);
        }
    }

    public SearchRequestBuilder getSearchRequestBuilder() {
        return this.searchRequestBuilder;
    }
}
