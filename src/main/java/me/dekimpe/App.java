package me.dekimpe;

import me.dekimpe.bolt.SaveRatesBolt;
import me.dekimpe.bolt.SaveTransactionsBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

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
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "bitcoin-rates-test");
    	spoutConfigBuilder.setGroupId("rates-consumer-tests-3");
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-rates-spout", new KafkaSpout<String, String>(spoutConfig));
        
        // Kafa : bitcoin-transactions-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "bitcoin-transactions-test");
    	spoutConfigBuilder.setGroupId("transactions-consumer-tests-3");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-transactions-spout", new KafkaSpout<String, String>(spoutConfig));
        
        /* Kafa : bitcoin-blocks-test
        spoutConfigBuilder = KafkaSpoutConfig.builder("storm-nimbus:9092", "bitcoin-blocks-test");
    	spoutConfigBuilder.setGroupId("blocks-consumer-tests");
    	spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("bitcoins-blocks-spout", new KafkaSpout<String, String>(spoutConfig));*/
        
        // Création d'un Bolt pour gérer les rates
        builder.setBolt("bitcoins-rates-bolt", new SaveRatesBolt())
                .shuffleGrouping("bitcoins-rates-spout");
        
        // Création d'un Bolt pour gérer les transactions
        builder.setBolt("bitcoins-transactions-bolt", new SaveTransactionsBolt())
                .shuffleGrouping("bitcoins-transactions-spout");
        
        /* Création d'un Bolt pour gérer les transactions
        builder.setBolt("bitcoins-blocks-bolt", new SaveBlocksBolt())
                .shuffleGrouping("bitcoins-blocks-spout");*/
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setNumWorkers(9);
    	String topologyName = "bitcoins-tests";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}

