/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.spout;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

/**
 *
 * @author Coreuh
 */
public class GetRatesSpout {
    
    private String topic = "bitcoin-rates-test";
    
    private String consumerGroup = "bitcoin-rates-consumers-test";
    
    private String zookeeper = "zoo1:2182,zoo2:2182,zoo3:2182";
    
    private String kafkaBrokers = "kafka1:9092,kafka2:9092,kafka3:9093";
    
    public GetRatesSpout(KafkaSpoutConfig kafkaSpoutConfig) {
        
        /* ZooKeeper connection string
        BrokerHosts hosts = new ZkHosts(zookeeper);

        //Creating SpoutConfig Object
        SpoutConfig spoutConfig = new SpoutConfig(hosts, 
           topicName, "/" + topicName UUID.randomUUID().toString());

        //convert the ByteBuffer to String.
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //Assign SpoutConfig to KafkaSpout.
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        super(kafkaSpoutConfig);
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder =
                KafkaSpoutConfig.builder(kafkaBrokers, topic);
        
        // On définit ici le groupe Kafka auquel va appartenir le spout
        spoutConfigBuilder.setGroupId(consumerGroup);
        // Création d'un objet KafkaSpoutConfig
        
        // Création d'un objet KafkaSpout
        
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();*/
    }
    
}
