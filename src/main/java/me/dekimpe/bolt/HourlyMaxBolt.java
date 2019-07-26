/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.bolt;

import java.util.Date;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 *
 * @author cdekimpe
 */
public class HourlyMaxBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "maxValue", "eurValue", "averageEur"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    // Input example : {"timestamp": 1563961571, "eur": 8734.6145}
    // Input example : {"timestamp": 1563961758, "amount": 0.00612958, "hash": "57fe6a1887f14d9df1036c8709a6daa1c5a2ccaae34a38ebb4235c5fb7386906"}
    @Override
    public void execute(TupleWindow inputWindow) {
        
        int timestamp = 0;
        int totalEurTuples = 0;
        double totalEurValue = 0;
        double maxBitValue = 0;
        double newAmount;
        
		
        for (Tuple input : inputWindow.get()) {
            // If it is a Rate
            if (input.contains("eur")) {
                totalEurValue += input.getDoubleByField("eur");
                totalEurTuples++;
            }
            // If it is a Transaction
            else if (input.contains("amount")) {
                newAmount = input.getDoubleByField("amount");
                if(newAmount > maxBitValue) {
                    maxBitValue = newAmount;
                }
            }
            // Get timestamp from last tuple
            timestamp = input.getIntegerByField("timestamp");
        }
        
        if (totalEurTuples == 0 || maxBitValue == 0) {
            for (Tuple input : inputWindow.get()) {
                outputCollector.fail(input);
            }
        } else {
            double averageEurValue = totalEurValue / totalEurTuples;
            double eurValue = maxBitValue * averageEurValue;
            for (Tuple input : inputWindow.get()) {
                outputCollector.ack(input);
            }
            outputCollector.emit(new Values(timestamp, maxBitValue, eurValue, averageEurValue));
        }
    }
    
}