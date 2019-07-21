/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.net.InetAddress;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 *
 * @author cdekimpe
 */
public class SaveRatesBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        throw new UnsupportedOperationException("Not supported yet.");
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
        // on startup
        Settings settings = Settings.builder()
                .put("cluster.name", "projet3").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic2"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic3"), 9300));

// on shutdown
        client.close();
    }
    
}
