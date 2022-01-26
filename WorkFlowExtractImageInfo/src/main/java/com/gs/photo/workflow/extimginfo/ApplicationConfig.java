package com.gs.photo.workflow.extimginfo;

import java.util.Properties;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.IgniteWrapper;
import com.gs.photo.common.workflow.iginite.IgniteSpringBean;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.gs.photos.serializers.FileToProcessSerializer;
import com.workflow.model.files.FileToProcess;

@Configuration
@ImportResource("file:${user.home}/config/cluster-client.xml")
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final String KAFAK_FILE_TO_PROCESS_DESERIALIZER = FileToProcessDeserializer.class.getName();
    private static final String KAFKA_FILE_TO_PROCESS_SERIALIZER   = FileToProcessSerializer.class.getName();

    protected static Logger     LOGGER                             = LoggerFactory.getLogger(ApplicationConfig.class);

    @Bean("producerForTransactionPublishingOnExifOrImageTopic")
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    @Scope("prototype")
    public Producer<String, Object> producerForTransactionPublishingOnExifOrImageTopic(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes,
        @Value("${kafka.producer.maxRequestSize}") int producerMaxRequestBytes

    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerMaxRequestBytes);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_MULTIPLE_SERIALIZER);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        Producer<String, Object> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean(name = "consumerForTopicWithFileToProcessValue")
    @Scope("prototype")
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    public Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue(
        @Value("${kafka.consumer.batchRecords}") int batchOfReadFiles,
        @Value("${group.id}") String groupId,
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${kafka.producer.maxRequestSize}") int producerRequestMaxBytes
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
        settings.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestMaxBytes);
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        settings.put("sasl.kerberos.service.name", "kafka");
        Consumer<String, FileToProcess> producer = new KafkaConsumer<>(settings);
        return producer;
    }

    @Bean
    @ConditionalOnClass(value = { IgniteSpringBean.class, IgniteConfiguration.class })
    public IgniteSpringBean igniteSpringBean(IgniteConfiguration igniteConfiguration) {
        final IgniteSpringBean igniteSpringBean = new IgniteSpringBean();
        igniteSpringBean.setConfiguration(igniteConfiguration);
        return igniteSpringBean;
    }

    @Bean
    @ConditionalOnClass(value = { org.apache.ignite.IgniteCache.class, IgniteSpringBean.class })
    public IgniteCache<String, byte[]> clientCache(IgniteSpringBean beanIgnite) {
        return new IgniteWrapper(beanIgnite);
    }

}
