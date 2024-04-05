package com.gs.photo.workflow.pubthumbimages;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.AbstractKafkaStreamApplication;
import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.common.workflow.SpecificKafkaStreamApplicationProperties;
import com.gs.photo.common.workflow.exif.ExifServiceImpl;
import com.gs.photo.common.workflow.exif.IExifService;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractKafkaStreamApplication {

    @Bean
    @ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = false)
    public IKafkaStreamProperties specificKafkaStreamApplicationProperties() {
        return new SpecificKafkaStreamApplicationProperties();
    }

    @Bean(name = "kafkaStreamTopologyProperties")
    public Properties kafkaStreamTopologyProperties(IKafkaStreamProperties kafkaStreamProperties) {
        return this.buildKafkaStreamProperties(kafkaStreamProperties);
    }

    @Bean
    @ConfigurationProperties(prefix = AbstractApplicationConfig.CONFIG_PREIFX_APPLICATION_SPECIFIC, ignoreUnknownFields = false)
    public SpecificApplicationProperties specificApplicationProperties() { return new SpecificApplicationProperties(); }

    @Bean
    public IExifService exifService(SpecificApplicationProperties properties) {
        return new ExifServiceImpl(properties.getExifFiles());
    }

    @Bean
    public Void processAndPublishThumbImages(
        IBeanPublishThumbImages beanPublishExifData,
        IKafkaStreamProperties applicationSpecificProperties,
        Properties kafkaStreamTopologyProperties
    ) {
        Topology topology = beanPublishExifData.buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, kafkaStreamTopologyProperties);
        if (applicationSpecificProperties.isCleanupRequired()) {
            kafkaStreams.cleanUp();
        }
        kafkaStreams.start();
        AbstractApplicationConfig.LOGGER.info(
            "started kafka gttreams {} ",
            topology.describe()
                .toString());

        Runtime.getRuntime()
            .addShutdownHook(new Thread(kafkaStreams::close));
        return null;
    }
}
