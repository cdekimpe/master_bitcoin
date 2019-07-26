package me.dekimpe.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class BestMinerBolt extends BaseWindowedBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
            int timestamp = 0;
            int totalEurTuples = 0;
            double totalEurValue = 0;
            HashMap<String, Double> miners = new HashMap<>();

            // Collect stats for all tuples
            // Block declarer : "foundBy", "timestamp", "reward", "hash"
            // Block types : String, Integer, Double, String
            // Rate declarer : "timestamp", "eur"
            // Rate types : String, Double
            Integer tupleCount = 0;
            for (Tuple input : inputWindow.get()) {
                if (input.contains("eur")) {
                    totalEurValue += input.getDoubleByField("eur");
                    totalEurTuples++;
                } else {
                    String foundBy = input.getStringByField("foundBy");
                    Double reward = input.getDoubleByField("reward");
                    miners.putIfAbsent(foundBy, 0.0d);
                    miners.put(foundBy, reward + miners.get(foundBy));
                }
                timestamp = input.getIntegerByField("timestamp");
                outputCollector.ack(input);
            }
            
            String bestMiner = "";
            double bestMinerValue = 0;
            for (Entry<String, Double> miner : miners.entrySet()){
                if(miner.getValue() > bestMinerValue) {
                    bestMiner = miner.getKey();
                    bestMinerValue = miner.getValue();
                }
            }
            
            double averageEur = totalEurValue / totalEurTuples;
            double eurValue = bestMinerValue * averageEur;
            
            outputCollector.emit(new Values(timestamp, bestMiner, bestMinerValue, eurValue, averageEur));            
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "miner", "value", "eurValue", "averageEur"));
	}
}
