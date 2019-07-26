package me.dekimpe;

import me.dekimpe.bolt.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException,
            InvalidTopologyException, AuthorizationException
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        // Kafa : bitcoin-rates-test
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("kafka1:9092", "topic-rates")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "consumer-rates");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-rates-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Kafa : bitcoin-transactions-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("kafka1:9092", "topic-transactions")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "consumer-transactions");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-transactions-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Kafa : bitcoin-blocks-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("kafka1:9092", "topic-blocks")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "consumer-blocks");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-blocks-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Parsings
        builder.setBolt("bitcoins-parsed-rates", new ParsingRatesBolt())
                .shuffleGrouping("bitcoins-rates-spout");
        builder.setBolt("bitcoins-parsed-transactions", new ParsingTransactionsBolt())
                .shuffleGrouping("bitcoins-transactions-spout");
        builder.setBolt("bitcoins-parsed-blocks", new ParsingBlocksBolt())
                .shuffleGrouping("bitcoins-blocks-spout");
        
        //Bitcoins Volumes Transfered
        builder.setBolt("bitcoins-volume-transfered", new HourlyVolumesBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000 * 60)))
                .shuffleGrouping("bitcoins-parsed-rates")
                .shuffleGrouping("bitcoins-parsed-transactions");
        
        /// Bitcoins Max Transfered
        builder.setBolt("bitoins-max-transfered", new HourlyMaxBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000 * 60)))
                .shuffleGrouping("bitcoins-parsed-rates")
                .shuffleGrouping("bitcoins-parsed-transactions");
        
        // Best Miner
        builder.setBolt("bitcoins-best-miner", new BestMinerBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000 * 60)))
                .shuffleGrouping("bitcoins-parsed-rates")
                .fieldsGrouping("bitcoins-parsed-blocks", new Fields("foundBy"));
        
        // Enregistrements des spouts dans ES à les des Save***Bolt
        builder.setBolt("save-rates-bolt", new SaveRatesBolt())
                .shuffleGrouping("bitcoins-parsed-rates");
        builder.setBolt("save-transactions-bolt", new SaveTransactionsBolt().withTumblingWindow(BaseWindowedBolt.Count.of(30)))
                .shuffleGrouping("bitcoins-parsed-transactions");
        builder.setBolt("save-blocks-bolt", new SaveBlocksBolt())
                .shuffleGrouping("bitcoins-parsed-blocks");
        builder.setBolt("save-volume-transfered", new SaveHourlyVolumesBolt())
                .shuffleGrouping("bitcoins-volume-transfered");
        builder.setBolt("save-max-bolt", new SaveHourlyMaxBolt())
                .shuffleGrouping("bitoins-max-transfered");
        builder.setBolt("save-best-miner", new SaveBestMinerBolt())
                .shuffleGrouping("bitcoins-best-miner");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(4);
    	String topologyName = "Bitcoin-Management";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}

