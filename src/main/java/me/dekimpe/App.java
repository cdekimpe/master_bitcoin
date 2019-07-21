package me.dekimpe;

import me.dekimpe.bolt.SaveRatesBolt;
import me.dekimpe.spout.GetRatesSpout;
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
        
        
        // Création d'un Bolt pour gérer les rates
        builder.setBolt("bitcoins-rates-bolt", new SaveRatesBolt())
                .shuffleGrouping("bitcoin-rates-spout");
        
        StormTopology topology = builder.createTopology();
        Config config = new Config();
    	config.setMessageTimeoutSecs(60*30);
    	String topologyName = "bitcoins";
        
        StormSubmitter.submitTopology(topologyName, config, topology);
    }
}
