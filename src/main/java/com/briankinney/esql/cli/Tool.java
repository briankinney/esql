package com.briankinney.esql.cli;

import com.briankinney.esql.client.EsqlClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Tool {
    public static void main(String[] args) {
        // TODO: parse settings from args
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

        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByAddress("localhost", new byte[]{0, 0, 0, 0});
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }

        TransportClient transportClient = new PreBuiltTransportClient(Settings.builder()
                .put("cluster.name", "docker-cluster").build())
                .addTransportAddress(new InetSocketTransportAddress(esAddress, 9300));

        SearchResponse response = new EsqlClient(transportClient).executeSearch(inputStream);

        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
