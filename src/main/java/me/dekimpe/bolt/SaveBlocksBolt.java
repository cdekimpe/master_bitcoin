/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
                .put("cluster.name", "projet3")
                .put("client.transport.sniff", "true").build();
        
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic2"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elastic3"), 9300));
        
        JSONParser jsonParser = new JSONParser();
	JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
	String contract = (String)obj.get("contract_name");
	Long availableStands = (Long)obj.get("available_bike_stands");
	Long stationNumber = (Long)obj.get("number");
		
	outputCollector.emit(new Values(contract, stationNumber, availableStands));
	outputCollector.ack(input);
        
        Map<String, Object> json = new HashMap<>();
        json.put("timestamp", input.getStringByField("timestamp"));
        json.put("eur", input.getStringByField("eur"));

        // Shutdown connection to ES cluster
        client.close();
    }
    
}
