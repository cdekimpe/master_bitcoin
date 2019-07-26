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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 *
 * @author cdekimpe
 */
public class SaveBlocksBolt extends BaseRichBolt {
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
        // Input example : {"foundBy": "F2Pool", "timestamp": 1563961597, "reward": 12.5, "hash": "000000000000000000086c5c7ffcfd31431fbeaaed62c582e72d79db49f07fac"}
        String json = "{\"foundBy\": \"" + input.getStringByField("foundBy") + "\", "
                + "\"timestamp\": " + input.getLongByField("timestamp") + ", "
                + "\"reward\": " + input.getFloatByField("reward") + ", "
                + "\"hash\": \"" + input.getStringByField("hash") + "\"}";
        
        IndexResponse response = client.prepareIndex(ElasticConfig.INDEX, "block")
                .setSource(json, XContentType.JSON)
                .get();

        // Shutdown connection to ES cluster
        client.close();
    }
    
}
