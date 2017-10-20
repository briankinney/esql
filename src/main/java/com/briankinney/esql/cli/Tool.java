package com.briankinney.esql.cli;

import com.briankinney.esql.QueryBuilderListener;
import com.briankinney.esql.esqlLexer;
import com.briankinney.esql.esqlParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class Tool {
    public static void main(String[] args) {
        // InputStream inputStream = new ByteArrayInputStream("SELECT _status.actions.trigger_alert.ack.state FROM .watches LIMIT 10;".getBytes());
        InputStream inputStream = null;
        if (args.length == 0 || args[0].equals("-")) {
            inputStream = System.in;
        } else {
            try {
                inputStream = new FileInputStream(args[0]);
            }
            catch (FileNotFoundException e) {
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

        ParseTreeWalker.DEFAULT.walk(new QueryBuilderListener(), searchQuery);
    }
}
