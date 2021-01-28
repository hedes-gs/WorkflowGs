package com.gs.photo.workflow.dupcheck;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    @Bean(name = "kafkaStreamProperties")
    public Properties kafkaStreamProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
        @Value("${application.kafkastreams.id}") String applicationId,
        @Value("${kafka.pollTimeInMillisecondes}") int pollTimeInMillisecondes,
        @Value("${kafka.consumer.batchRecords}") int consumerBatch,
        @Value("${kafka.stream.commit.interval.ms}") int commitIntervalIms,
        @Value("${kafka.stream.metadata.age.ms}") int metaDataAgeIms,
        @Value("${kafka.stream.nb.of.threads}") int nbOfThreads,
        @Value("${kafka.producer.maxBlockMsConfig}") int maxBlockMsConfig,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes,
        @Value("${kafka.producer.maxRequestSize}") int producerRequestMaxBytes,
        @Value("${kafka.consumer.retryBackoffMsmaxRequestSize}") int retryBackoffMs,
        @Value("${kafka.consumer.reconnectBackoffMs}") int reconnectBackoffMs,
        @Value("${kafka.consumer.heartbeatIntervalMs}") int heartbeatIntervalMs,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs
    ) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStreamDir);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalIms);
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metaDataAgeIms);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, nbOfThreads);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestMaxBytes);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        config.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        return config;
    }
}
