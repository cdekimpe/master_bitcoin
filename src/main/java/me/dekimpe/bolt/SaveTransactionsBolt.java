/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.net.InetAddress;
import java.util.Map;
import me.dekimpe.ElasticConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 *
 * @author cdekimpe
 */
public class SaveTransactionsBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    @Override
    public void execute(Tuple input) {
        try {
            process(input);
            outputCollector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }
    }
    
    private void process(Tuple input) throws Exception {
        
        // Create a connection to ES cluster
        Settings settings = Settings.builder()
                .put("cluster.name", ElasticConfig.CLUSTER_NAME)
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST1), ElasticConfig.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST2), ElasticConfig.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST3), ElasticConfig.PORT));
        
        // Récupération des données du input et transformation en JSON :
        // Input example : {"timestamp": 1563961758, "amount": 0.00612958, "hash": "57fe6a1887f14d9df1036c8709a6daa1c5a2ccaae34a38ebb4235c5fb7386906"}
        String head = "{\"index\":{}}";
        String json = "{\"timestamp\": " + input.getIntegerByField("timestamp") + ", "
                + "\"amount\": " + input.getDoubleByField("amount") + ", "
                + "\"hash\": \"" + input.getStringByField("hash") + "\"}";
        
        IndexResponse response = client.prepareIndex(ElasticConfig.INDEX, "transaction")
                .setSource(json, XContentType.JSON)
                .get();
        
        /*BulkRequestBuilder bulkRequest = client.prepareBulk();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex(ElasticConfig.INDEX, "transaction")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("message", "another post")
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }*/

        // Vérifier si la réponse est correcte
        // Sinon envoyer une exception pour signaler le mauvais traitement.
        
        // Shutdown connection to ES cluster
        client.close();
    }
    
}
