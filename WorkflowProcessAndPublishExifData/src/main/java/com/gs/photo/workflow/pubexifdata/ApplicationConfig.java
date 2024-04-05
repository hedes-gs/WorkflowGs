package com.gs.photo.workflow.pubexifdata;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.AbstractKafkaStreamApplication;
import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.common.workflow.SpecificKafkaStreamApplicationProperties;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractKafkaStreamApplication {

    private static Logger       LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);

    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

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
    public Void processAndPublishExifDataInit(
        IBeanPublishExifData beanPublishExifData,
        IKafkaStreamProperties applicationSpecificProperties,
        Properties kafkaStreamTopologyProperties
    ) {
        Topology topology = beanPublishExifData.buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, kafkaStreamTopologyProperties);
        if (applicationSpecificProperties.isCleanupRequired()) {
            kafkaStreams.cleanUp();
        }
        kafkaStreams.start();
        Runtime.getRuntime()
            .addShutdownHook(new Thread(kafkaStreams::close));

        return null;
    }

}
