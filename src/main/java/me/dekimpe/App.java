package me.dekimpe;

import me.dekimpe.bolt.*;
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
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "topic-rates");
    	spoutConfigBuilder.setGroupId("rates-consumer");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-rates-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Kafa : bitcoin-transactions-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "topic-transactions");
    	spoutConfigBuilder.setGroupId("transactions-consumer");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-transactions-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Kafa : bitcoin-blocks-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "topic-blocks");
    	spoutConfigBuilder.setGroupId("blocks-consumer-tests");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-blocks-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Bitcoins Volumes Transfered
        builder.setBolt("bitcoins-volume-transfered", new HourlyVolumesBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*1)))
                .shuffleGrouping("bitcoins-rates-spout")
                .shuffleGrouping("bitcoins-transactions-spout");
        
        // Bitcoins Max Transfered
        builder.setBolt("bitoins-max-transfered", new HourlyMaxBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*1)))
                .shuffleGrouping("bitcoins-rates-spout")
                .shuffleGrouping("bitcoins-transactions-spout");
        
        // Best Miner
        builder.setBolt("bitcoins-best-miner", new BestMinerBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000*60*1)))
                .shuffleGrouping("bitcoins-rates-spout")
                .fieldsGrouping("bitcoins-blocks-spout", new Fields("foundBy"));
        
        // Enregistrements des spouts dans ES à les des Save***Bolt
        builder.setBolt("bitcoins-rates-bolt", new SaveRatesBolt())
                .shuffleGrouping("bitcoins-rates-spout");
        builder.setBolt("bitcoins-transactions-bolt", new SaveTransactionsBolt())
                .shuffleGrouping("bitcoins-transactions-spout");
        builder.setBolt("bitcoins-blocks-bolt", new SaveBlocksBolt())
                .shuffleGrouping("bitcoins-blocks-spout");
        builder.setBolt("save-max-bolt", new SaveHourlyMaxBolt())
                .shuffleGrouping("bitoins-max-transfered");
        builder.setBolt("save-volume-transfered", new SaveHourlyVolumesBolt())
                .shuffleGrouping("bitcoins-volume-transfered");
        builder.setBolt("save-best-miner", new SaveBestMinerBolt())
                .shuffleGrouping("bitcoins-best-miner");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(9);
    	String topologyName = "bitcoin-management";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}

