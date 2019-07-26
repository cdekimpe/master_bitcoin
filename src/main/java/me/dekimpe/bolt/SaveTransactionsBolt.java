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
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 *
 * @author cdekimpe
 */
public class SaveTransactionsBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        try {
            process(inputWindow);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void process(TupleWindow inputWindow) throws Exception {
        
        // Create a connection to ES cluster
        Settings settings = Settings.builder()
                .put("cluster.name", ElasticConfig.CLUSTER_NAME)
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST1), ElasticConfig.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST2), ElasticConfig.PORT))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ElasticConfig.HOST3), ElasticConfig.PORT));
        
        String json;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Tuple input : inputWindow.get()) {
            json = "{\"timestamp\": " + input.getIntegerByField("timestamp") + ", "
                    + "\"amount\": " + input.getDoubleByField("amount") + ", "
                    + "\"hash\": \"" + input.getStringByField("hash") + "\"}";
            bulkRequest.add(client.prepareIndex(ElasticConfig.INDEX, "transaction")
                    .setSource(json, XContentType.JSON));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        for (Tuple input : inputWindow.get()) {
            if (bulkResponse.hasFailures()) {
                outputCollector.fail(input);
            } else {
                outputCollector.ack(input);
            }
        }

        client.close();
    }
    
}
