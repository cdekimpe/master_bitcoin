/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

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
    
    public void process(Tuple input) throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("host1"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9300));

        // on shutdown
        client.close();
    }
    
}
