package com.gs.photo.workflow.dupcheck;

import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.KafkaConsumerProperties;
import com.gs.photo.common.workflow.KafkaProducerProperties;
import com.gs.photo.workflow.dupcheck.config.ApplicationSpecificProperties;
import com.gs.photo.workflow.dupcheck.config.IKafkaStreamProperties;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    @Bean
    @ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = false)
    public IKafkaStreamProperties applicationSpecificProperties() { return new ApplicationSpecificProperties(); }

    @Bean(name = "kafkaStreamTopologyProperties")
    public Properties kafkaStreamTopologyProperties(IKafkaStreamProperties kafkaStreamProperties) {
        Properties config = new Properties();
        config.put("sasl.kerberos.service.name", "kafka");
        final KafkaConsumerProperties kafkaConsumerProperties = kafkaStreamProperties.getConsumersType()
            .get("kafka-stream");
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, kafkaConsumerProperties.retryBackoffMs());
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, kafkaConsumerProperties.reconnectBackoffMs());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamProperties.getApplicationId());
        config.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaStreamProperties.getBootStrapServers()
                .stream()
                .collect(Collectors.joining(",")));
        config.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStreamProperties.getKafkaStreamDir());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kafkaStreamProperties.getCommitIntervalIms());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, kafkaStreamProperties.getCacheMaxBytesBuffering());
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStreamProperties.getNbOfThreads());

        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, kafkaConsumerProperties.consumerFetchMaxBytes());
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaConsumerProperties.heartbeatIntervallMs());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConsumerProperties.sessionTimeoutMs());
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, kafkaStreamProperties.getMetaDataMaxAgeInMs());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConsumerProperties.maxPollIntervallMs());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConsumerProperties.maxPollRecords());

        final KafkaProducerProperties kafkaProducerProperties = kafkaStreamProperties.getProducersType()
            .get("kafka-stream");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaProducerProperties.maxBlockMsConfig());
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaProducerProperties.maxRequestSize());
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        return config;
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
