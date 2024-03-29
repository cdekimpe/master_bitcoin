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
public class ParsingRatesBolt extends BaseRichBolt {
    
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
    
    // Input example : {"timestamp": 1563961571, "eur": 8734.6145}
    private void process(Tuple input) {
        JSONObject obj = new org.json.JSONObject(input.getStringByField("value"));
        Integer timestamp = (Integer) obj.get("timestamp");
        Double amount = (Double) obj.get("eur");
        outputCollector.emit(new Values(timestamp, amount));
        outputCollector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "eur"));
    }

    
}

