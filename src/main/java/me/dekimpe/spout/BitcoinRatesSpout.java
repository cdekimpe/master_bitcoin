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
public class BitcoinRatesSpout extends KafkaSpout {
    
    public BitcoinRatesSpout(KafkaSpoutConfig kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder =
                KafkaSpoutConfig.builder("kafka1:9092", "bitcoin-rates-test");
        
        // On définit ici le groupe Kafka auquel va appartenir le spout
        spoutConfigBuilder.setGroupId("bitcoin-rates-consumers-test");
        // Création d'un objet KafkaSpoutConfig
        
        // Création d'un objet KafkaSpout
        builder.setSpout("bitcoin-rates-spout", new KafkaSpout<String, String>(spoutConfig));
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    }
    
}
