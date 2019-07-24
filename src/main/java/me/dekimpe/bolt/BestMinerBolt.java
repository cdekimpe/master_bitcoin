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
            long timestamp = 0;
            int totalEurTuples = 0;
            float totalEurValue = 0;
            HashMap<String, Float> miners = new HashMap<>();

            // Collect stats for all tuples
            // Block declarer : "foundBy", "timestamp", "reward", "hash"
            // Block types : String, Long, Float, String
            // Rate declarer : "timestamp", "eur"
            // Rate types : String, Float
            Integer tupleCount = 0;
            for (Tuple input : inputWindow.get()) {
                if (input.contains("eur")) {
                    totalEurValue += input.getFloatByField("eur");
                    totalEurTuples++;
                } else {
                    String foundBy = input.getStringByField("foundBy");
                    Float reward = input.getFloatByField("reward");
                    miners.putIfAbsent(foundBy, 0.0f);
                    miners.put(foundBy, reward + miners.get(foundBy));
                }
                timestamp = input.getLongByField("timestamp");
                outputCollector.ack(input);
            }
            
            String bestMiner = "";
            float bestMinerValue = 0;
            for (Entry<String, Float> miner : miners.entrySet()){
                if(miner.getValue() > bestMinerValue) {
                    bestMiner = miner.getKey();
                    bestMinerValue = miner.getValue();
                }
            }
            
            float averageEur = totalEurValue / totalEurTuples;
            float eurValue = bestMinerValue * averageEur;
            
            outputCollector.emit(new Values(timestamp, bestMiner, bestMinerValue, eurValue, averageEur));            
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "miner", "value", "eurValue", "averageEur"));
	}
}
