package com.gs.photo.common.workflow;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.photo.common.workflow.exif.ExifServiceImpl;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photos.serializers.ExchangedDataSerializer;
import com.gs.photos.serializers.MultipleSerializers;
import com.gs.photos.serializers.WfEventsSerializer;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.events.WfEvents;

@Configuration
public abstract class AbstractApplicationConfig {

    private static final String             IGNITE_SPRING_BEAN                           = "ignite-spring-bean";

    private static final String             CONFIG_CLUSTER_CLIENT_XML                    = "config/cluster-client.xml";

    protected static final org.slf4j.Logger LOGGER                                       = LoggerFactory
        .getLogger(AbstractApplicationConfig.class);

    private static final String             KAFKA_EXCHANGED_DATA_SERIALIZER              = ExchangedDataSerializer.class
        .getName();
    public final static String              KAFKA_STRING_DESERIALIZER                    = org.apache.kafka.common.serialization.StringDeserializer.class
        .getName();
    public final static String              KAFKA_STRING_SERIALIZER                      = org.apache.kafka.common.serialization.StringSerializer.class
        .getName();
    public final static String              KAFKA_WFEVENTS_SERIALIZER                    = WfEventsSerializer.class
        .getName();
    public final static String              KAFKA_LONG_SERIALIZER                        = org.apache.kafka.common.serialization.LongSerializer.class
        .getName();
    public final static String              KAFKA_BYTES_DESERIALIZER                     = org.apache.kafka.common.serialization.ByteArrayDeserializer.class
        .getName();
    public final static String              KAFKA_BYTE_SERIALIZER                        = org.apache.kafka.common.serialization.ByteArraySerializer.class
        .getName();
    public final static String              HBASE_IMAGE_THUMBNAIL_SERIALIZER             = com.gs.photos.serializers.HbaseImageThumbnailSerializer.class
        .getName();
    public final static String              HBASE_IMAGE_THUMBNAIL_DESERIALIZER           = com.gs.photos.serializers.HbaseImageThumbnailDeserializer.class
        .getName();

    public final static String              HBASE_IMAGE_THUMBNAIL_KEY_DESERIALIZER       = com.gs.photos.serializers.HbaseImageThumbnailKeyDeserializer.class
        .getName();
    public final static String              HBASE_IMAGE_EXIF_DATA_DESERIALIZER           = com.gs.photos.serializers.HbaseExifDataDeserializer.class
        .getName();
    public final static String              HBASE_IMAGE_EXIF_DATA_SERIALIZER             = com.gs.photos.serializers.HbaseExifDataSerializer.class
        .getName();
    public final static String              HBASE_IMAGE_EXIF_DATA_OF_IMAGES_DESERIALIZER = com.gs.photos.serializers.HbaseExifDataOfImagesDeserializer.class
        .getName();
    public final static String              HBASE_IMAGE_EXIF_DATA_OF_IMAGES_SERIALIZER   = com.gs.photos.serializers.HbaseExifDataOfImagesSerializer.class
        .getName();
    public final static String              HBASE_DATA_DESERIALIZER                      = com.gs.photos.serializers.HbaseDataDeserializer.class
        .getName();
    public final static String              HBASE_DATA_SERIALIZER                        = com.gs.photos.serializers.HbaseDataSerializer.class
        .getName();
    public final static String              CACHE_NAME                                   = "start-raw-files";

    public static final String              KAFKA_MULTIPLE_SERIALIZER                    = MultipleSerializers.class
        .getName();

    @Value("${application.id}")
    protected String                        applicationId;

    @Bean
    @ConditionalOnProperty(name = "producer.string.string", havingValue = "true")
    public Producer<String, String> producerForPublishingOnImageTopic(
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, String> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean("producerForPublishingWfEvents")
    @Scope("prototype")
    public Producer<String, WfEvents> producerForPublishingWfEvents(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_WFEVENTS_SERIALIZER);
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, WfEvents> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean
    @ConditionalOnProperty(name = "producer.string.string", havingValue = "true")
    public Producer<String, String> producerForPublishingOnStringTopic(
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        AbstractApplicationConfig.LOGGER.info("creating producer string string with config {} ", settings.toString());
        Producer<String, String> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean("producerForPublishingInModeTransactionalOnStringTopic")
    @ConditionalOnProperty(name = "producer.string.string.transactional", havingValue = "true")
    public Producer<String, String> producerForPublishingInModeTransactionalOnStringTopic(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        Producer<String, String> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean("producerForPublishingInModeTransactionalOnLongTopic")
    @ConditionalOnProperty(name = "producer.string.long.transactional", havingValue = "true")
    public Producer<String, Long> producerForPublishingInModeTransactionalOnLongTopic(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_LONG_SERIALIZER);
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, Long> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean(name = "producerForPublishingOnExifTopic")
    @ConditionalOnProperty(name = "producer.string.exchangedData", havingValue = "true")
    public Producer<String, ExchangedTiffData> producerForPublishingOnExifTopic(
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            AbstractApplicationConfig.KAFKA_EXCHANGED_DATA_SERIALIZER);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, ExchangedTiffData> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean(name = "producerForPublishingOnJpegImageTopic")
    @ConditionalOnProperty(name = "producer.string.exchangedData", havingValue = "true")
    public Producer<String, byte[]> producerForPublishingOnJpegImageTopic(
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_BYTE_SERIALIZER);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, byte[]> producer = new KafkaProducer<>(settings);
        return producer;
    }

    @Bean(name = "consumerForTopicWithStringKey")
    @ConditionalOnProperty(name = "consumer.string.string", havingValue = "true")
    public Consumer<String, String> consumerForTopicWithStringKey(
        @Value("${group.id}") String groupId,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, this.applicationId);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        settings.put("sasl.kerberos.service.name", "kafka");
        Consumer<String, String> producer = new KafkaConsumer<>(settings);
        return producer;
    }

    @Bean(name = "consumerForTransactionalCopyForTopicWithStringKey")
    @ConditionalOnProperty(name = "consumer.string.string.transactional", havingValue = "true")
    public Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${copy.group.id}") String copyGroupId
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, "tr-" + this.applicationId);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, copyGroupId);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        Consumer<String, String> consumer = new KafkaConsumer<>(settings);
        return consumer;
    }

    @Bean(name = "consumerCommonKafkaProperties")
    public Properties consumerCommonKafkaProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${copy.group.id}") String copyGroupId
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, copyGroupId);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        return settings;
    }

    @Bean(name = "threadPoolTaskExecutor")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        // threadPoolTaskExecutor.setDaemon(false);
        threadPoolTaskExecutor.setCorePoolSize(6);
        threadPoolTaskExecutor.setMaxPoolSize(64);
        threadPoolTaskExecutor.setThreadNamePrefix("wf-task-executor");
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    @Bean
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    public CountDownLatch shutdownCoseLatch() { return new CountDownLatch(1); }

    @Bean
    @ConditionalOnProperty(name = "exifservice.is.used", havingValue = "true")
    public IExifService exifService(@Value("${exif.files}") List<String> exifFiles) {
        return new ExifServiceImpl(exifFiles);
    }

}