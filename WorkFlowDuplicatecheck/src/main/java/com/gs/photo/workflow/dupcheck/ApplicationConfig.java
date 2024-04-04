package com.gs.photo.workflow.dupcheck;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractKafkaStreamApplication;
import com.gs.photo.common.workflow.SpecificKafkaStreamApplicationProperties;
import com.gs.photo.common.workflow.IKafkaStreamProperties;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractKafkaStreamApplication {

    @Bean
    @ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = false)
    public IKafkaStreamProperties applicationSpecificProperties() { return new SpecificKafkaStreamApplicationProperties(); }

    @Bean(name = "kafkaStreamTopologyProperties")
    public Properties kafkaStreamTopologyProperties(IKafkaStreamProperties kafkaStreamProperties) {
        return buildKafkaStreamProperties(kafkaStreamProperties);
    }

    @Bean
    public Void duplicateCheckInit(
        IDuplicateCheck duplicateCheck,
        IKafkaStreamProperties applicationSpecificProperties,
        Properties kafkaStreamTopologyProperties
    ) {
        Topology topology = duplicateCheck.buildKafkaStreamsTopology();
        KafkaStreams ks = new KafkaStreams(topology, kafkaStreamTopologyProperties);

        if (applicationSpecificProperties.isCleanupRequired()) {
            ks.cleanUp();
        }
        ks.start();
        return null;
    }

}
