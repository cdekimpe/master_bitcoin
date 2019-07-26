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
public class ParsingBlocksBolt extends BaseRichBolt {
    
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
    
    // Input example : {"foundBy": "F2Pool", "timestamp": 1563961597, "reward": 12.5, "hash": "000000000000000000086c5c7ffcfd31431fbeaaed62c582e72d79db49f07fac"}
    private void process(Tuple input)  {
        JSONObject obj = new JSONObject(input.getStringByField("value"));
        Integer timestamp = (Integer) obj.get("timestamp");
        Double reward = (Double) obj.get("reward");
        String hash = (String) obj.get("hash");
        String foundBy = (String) obj.get("foundBy");
        
        outputCollector.emit(new Values(foundBy, timestamp, reward, hash));
        outputCollector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("foundBy", "timestamp", "reward", "hash"));
    }

    
}

