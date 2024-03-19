package com.gs.photo.workflow.copyfiles;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.workflow.model.files.FileToProcess;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    protected static final org.slf4j.Logger LOGGER                             = LoggerFactory
        .getLogger(ApplicationConfig.class);

    private static final String             KAFAK_FILE_TO_PROCESS_DESERIALIZER = FileToProcessDeserializer.class
        .getName();

    @Bean(name = "consumerForTopicWithFileToProcessValue")
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    public Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue(
        @Value("${kafka.consumer.batchRecords}") int batchOfReadFiles,
        @Value("${group.id}") String groupId,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.maxPollIntervalMsConfig}") int maxPollIntervalMsConfig
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ApplicationConfig.KAFAK_FILE_TO_PROCESS_DESERIALIZER);
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, this.applicationId);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchOfReadFiles);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMsConfig);

        Consumer<String, FileToProcess> producer = new KafkaConsumer<>(settings);
        return producer;
    }

    @Bean
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    public Producer<String, Object> producerForTopicWithFileToProcessOrEventValue(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        String hostname = null;
        try {
            InetAddress ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_MULTIPLE_SERIALIZER);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId + "-" + hostname);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);

        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.BATCH_SIZE_CONFIG, 100 * 1204);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        ApplicationConfig.LOGGER.info("creating producer string string with config {} ", settings.toString());
        Producer<String, Object> producer = new KafkaProducer<>(settings);
        return producer;
    }

}
