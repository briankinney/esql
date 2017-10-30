package com.github.briankinney.esql.cli;

import com.github.briankinney.esql.client.EsqlClient;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Tool {

    private static OptionParser buildOptionParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("f", "File containing query to execute")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("-");
        parser.accepts("o", "Output file")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("-");
        parser.accepts("c", "Cluster name")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("elasticsearch");
        parser.accepts("h", "Host name")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("localhost");
        parser.accepts("p", "Port number")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(9300);

        // TODO: output format
        // TODO: security options

        return parser;
    }

    public static void main(String[] args) {

        OptionParser parser = buildOptionParser();

        OptionSet options = parser.parse(args);

        String sqlFileName = (String) options.valueOf("f");
        String outputFileName = (String) options.valueOf("o");
        String clusterName = (String) options.valueOf("c");
        String hostName = (String) options.valueOf("h");
        Integer portNumber = (Integer) options.valueOf("p");

        InputStream inputStream = null;
        if (sqlFileName.equals("-")) {
            inputStream = System.in;
        } else {
            try {
                inputStream = new FileInputStream(sqlFileName);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        OutputStream outputStream = null;
        if (outputFileName.equals("-")) {
            outputStream = System.out;
        } else {
            try {
                outputStream = new FileOutputStream(outputFileName);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByName(hostName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }

        TransportClient transportClient = new PreBuiltTransportClient(Settings.builder()
                .put("cluster.name", clusterName).build())
                .addTransportAddress(new InetSocketTransportAddress(esAddress, portNumber));

        SearchResponse response = new EsqlClient(transportClient).executeSearch(inputStream);

        try {
            for (SearchHit hit : response.getHits()) {
                outputStream.write(hit.getSourceAsString().getBytes());
                outputStream.write("\n".getBytes());
            }
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
