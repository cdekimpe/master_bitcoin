/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
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
public class SaveRatesBolt extends BaseRichBolt {
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
                .put("cluster.name", "projet3")
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("storm-supervisor-1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("storm-supervisor-2"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("storm-supervisor-3"), 9300));
        
        IndexResponse response = client.prepareIndex("bitcoin-test", "rates")
                .setSource(input.getStringByField("value"), XContentType.JSON)
                .get();

        // Vérifier si la réponse est correcte
        // Sinon envoyer une exception pour signaler le mauvais traitement.
        
        // Shutdown connection to ES cluster
        client.close();
    }
    
}
