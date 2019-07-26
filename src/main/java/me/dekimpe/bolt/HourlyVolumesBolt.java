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
public class HourlyVolumesBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "totalBitcoin", "eurValue", "averageEur"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        outputCollector = oc;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int timestamp = 0;
        int totalEurTuples = 0;
        double totalEurValue = 0;
        double totalBitValue = 0;
		
        for (Tuple input : inputWindow.get()) {
            // If it is a Rate
            if (input.contains("eur")) {
                totalEurValue += input.getDoubleByField("eur");
                totalEurTuples++;
            }
            // If it is a Transaction
            else if (input.contains("amount")) {
                totalBitValue += input.getDoubleByField("amount");
            }
            // Get timestamp from last tuple
            timestamp = input.getIntegerByField("timestamp");
        }
        
        if (totalEurTuples == 0 || totalBitValue == 0) {
            for (Tuple input : inputWindow.get()) {
                outputCollector.fail(input);
            }
        } else {
            double averageEurValue = totalEurValue / totalEurTuples;
            double eurValue = totalBitValue * averageEurValue;
            for (Tuple input : inputWindow.get()) {
                outputCollector.ack(input);
            }
            outputCollector.emit(new Values(timestamp, totalBitValue, eurValue, averageEurValue));
        }
    }
    
}