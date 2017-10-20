package com.briankinney.esql.cli;

import com.briankinney.esql.QueryBuilderListener;
import com.briankinney.esql.esqlLexer;
import com.briankinney.esql.esqlParser;
import org.antlr.v4.runtime.*;

import java.io.InputStream;

public class Tool {
    public static void main(String[] args) {
        InputStream inputStream = System.in;
        CharStream charStream = new UnbufferedCharStream(inputStream);
        TokenSource tokenSource = new esqlLexer(charStream);
        TokenStream tokenStream = new CommonTokenStream(tokenSource);
        esqlParser parser = new esqlParser(tokenStream);
        parser.addParseListener(new QueryBuilderListener());
        parser.search_query();
    }
}
