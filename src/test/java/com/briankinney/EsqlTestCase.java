package com.briankinney;

import com.briankinney.esql.client.EsqlClient;
import junit.framework.TestCase;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

class EsqlTestCase {

    EsqlClient esqlClient;
    TransportClient transportClient;

    EsqlTestCase() {
        InetAddress esAddress;
        try {
            esAddress = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }

        this.transportClient = new PreBuiltTransportClient(Settings.builder()
                .put("cluster.name", "docker-cluster").build())
                .addTransportAddress(new InetSocketTransportAddress(esAddress, 9300));

        this.esqlClient = new EsqlClient(this.transportClient);
    }

    String randomIndexName() {
        return "random-name-lul";
    }

    void createMessagesIndex(String indexName) {
        CreateIndexRequestBuilder createIndexActionBuilder =
                CreateIndexAction.INSTANCE.newRequestBuilder(this.transportClient);
        createIndexActionBuilder.setIndex(indexName).addMapping("messages",
                "from", "type=keyword",
                "to", "type=keyword",
                "title", "type=text",
                "body", "type=text",
                "timestamp", "type=long");
        createIndexActionBuilder.execute().actionGet();
    }

    void deleteIndex(String indexName) {
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(this.transportClient, DeleteIndexAction.INSTANCE, indexName);
        deleteIndexRequestBuilder.execute().actionGet();
    }

    void addMessage(String indexName, String from, String to, String title, String body, long timestamp) {
        IndexRequestBuilder indexRequestBuilder =
                new IndexRequestBuilder(this.transportClient, IndexAction.INSTANCE, indexName);
        indexRequestBuilder.setType("messages").setSource(
                "from", from,
                "to", to,
                "title", title,
                "body", body,
                "timestamp", timestamp);
        indexRequestBuilder.execute().actionGet();
    }
}
