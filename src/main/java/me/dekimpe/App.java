package me.dekimpe;

import me.dekimpe.bolt.BitcoinRatesBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder =
                KafkaSpoutConfig.builder("kafka1:9092", "bitcoin-rates-test");
        
        // On définit ici le groupe Kafka auquel va appartenir le spout
        spoutConfigBuilder.setGroupId("bitcoin-rates-consumers-test");
        // Création d'un objet KafkaSpoutConfig
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        // Création d'un objet KafkaSpout
        builder.setSpout("bitcoin-rates", new KafkaSpout<String, String>(spoutConfig));
        
        // Création d'un Bolt pour gérer les rates
        builder.setBolt("bitcoins-rates", new BitcoinRatesBolt())
                .shuffleGrouping("bitcoin-rates");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
    	config.setMessageTimeoutSecs(60*30);
    	String topologyName = "bitcoins";
        if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(topologyName, config, topology);
    	}
    	else {
    		LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(topologyName, config, topology);
    	}
    }
}
