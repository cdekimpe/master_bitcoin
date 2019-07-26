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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

/**
 *
 * @author cdekimpe
 */
public class ParsingTransactionsBolt extends BaseRichBolt {
    
    private OutputCollector outputCollector;

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
    
    // Input example : {"timestamp": 1563961758, "amount": 0.00612958, "hash": "57fe6a1887f14d9df1036c8709a6daa1c5a2ccaae34a38ebb4235c5fb7386906"}
    private void process(Tuple input)  {
        JSONObject obj = new JSONObject(input.getStringByField("value"));
        String hash = (String) obj.get("hash");
        Integer timestamp = (Integer) obj.get("timestamp");
        Double amount = (Double) obj.get("amount");
        
        String json = "{\"timestamp\": " + timestamp + ", "
                + "\"amount\": " + amount + ", "
                + "\"hash\": \"" + hash + "\"}";
        
        outputCollector.emit(new Values(timestamp, amount, hash));
        outputCollector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "amount", "hash"));
    }

    
}
