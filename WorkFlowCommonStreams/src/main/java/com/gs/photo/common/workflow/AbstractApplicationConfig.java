package com.gs.photo.common.workflow;

import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.LoggerFactory;

import com.gs.photos.serializers.ExchangedDataSerializer;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.gs.photos.serializers.FileToProcessSerializer;
import com.gs.photos.serializers.ImportEventDeserializer;
import com.gs.photos.serializers.MultipleSerializers;
import com.gs.photos.serializers.WfEventsSerializer;
import com.workflow.model.HbaseData;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

public abstract class AbstractApplicationConfig {

    private static final String                                      IGNITE_SPRING_BEAN                           = "ignite-spring-bean";

    private static final String                                      CONFIG_CLUSTER_CLIENT_XML                    = "config/cluster-client.xml";

    protected static final org.slf4j.Logger                          LOGGER                                       = LoggerFactory
        .getLogger(AbstractApplicationConfig.class);

    private static final String                                      KAFKA_EXCHANGED_DATA_SERIALIZER              = ExchangedDataSerializer.class
        .getName();
    public final static Class<? extends Deserializer<String>>        KAFKA_STRING_DESERIALIZER                    = org.apache.kafka.common.serialization.StringDeserializer.class;
    public final static Class<? extends Serializer<String>>          KAFKA_STRING_SERIALIZER                      = org.apache.kafka.common.serialization.StringSerializer.class;
    public static final Class<? extends Serializer<HbaseData>>       KAFKA_MULTIPLE_SERIALIZER                    = MultipleSerializers.class;
    public static final Class<? extends Deserializer<ImportEvent>>   KAFKA_IMPORT_EVENT_DESERIALIZER              = ImportEventDeserializer.class;
    public static final Class<? extends Deserializer<FileToProcess>> KAFKA_FILE_TO_PROCESS_DESERIALIZER           = FileToProcessDeserializer.class;
    public static final Class<? extends Serializer<FileToProcess>>   KAFKA_FILE_TO_PROCESS_SERIALIZER             = FileToProcessSerializer.class;

    public final static String                                       KAFKA_WFEVENTS_SERIALIZER                    = WfEventsSerializer.class
        .getName();
    public final static String                                       KAFKA_LONG_SERIALIZER                        = org.apache.kafka.common.serialization.LongSerializer.class
        .getName();
    public final static String                                       KAFKA_BYTES_DESERIALIZER                     = org.apache.kafka.common.serialization.ByteArrayDeserializer.class
        .getName();
    public final static String                                       KAFKA_BYTE_SERIALIZER                        = org.apache.kafka.common.serialization.ByteArraySerializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_THUMBNAIL_SERIALIZER             = com.gs.photos.serializers.HbaseImageThumbnailSerializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_THUMBNAIL_DESERIALIZER           = com.gs.photos.serializers.HbaseImageThumbnailDeserializer.class
        .getName();

    public final static String                                       HBASE_IMAGE_THUMBNAIL_KEY_DESERIALIZER       = com.gs.photos.serializers.HbaseImageThumbnailKeyDeserializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_EXIF_DATA_DESERIALIZER           = com.gs.photos.serializers.HbaseExifDataDeserializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_EXIF_DATA_SERIALIZER             = com.gs.photos.serializers.HbaseExifDataSerializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_EXIF_DATA_OF_IMAGES_DESERIALIZER = com.gs.photos.serializers.HbaseExifDataOfImagesDeserializer.class
        .getName();
    public final static String                                       HBASE_IMAGE_EXIF_DATA_OF_IMAGES_SERIALIZER   = com.gs.photos.serializers.HbaseExifDataOfImagesSerializer.class
        .getName();
    public final static String                                       HBASE_DATA_DESERIALIZER                      = com.gs.photos.serializers.HbaseDataDeserializer.class
        .getName();
    public final static String                                       HBASE_DATA_SERIALIZER                        = com.gs.photos.serializers.HbaseDataSerializer.class
        .getName();
    public final static String                                       CACHE_NAME                                   = "start-raw-files";

    protected static final String                                    ON_THE_FLY_CONSUMER_TYPE                     = "on_the_fly_consumer_type";
    protected static final String                                    ON_THE_FLY_PRODUCER_TYPE                     = "on_the_fly_producer_type";
    protected static final String                                    MEDIUM_PRODUCER_TYPE                         = "medium_producer_type";

    public static record KafkaClientConsumer(
        String consumerType,
        String groupId,
        String instanceGroupId
    ) {}

    public <K, V> IKafkaProducerFactory<K, V> kafkaProducerFactory(IKafkaProperties kafkaProperties) {
        return (consumerType, keySerializer, valueSerializer) -> this
            .buildProducer(kafkaProperties, consumerType, keySerializer, valueSerializer);
    }

    public <K, V> IKafkaConsumerFactory<K, V> kafkaConsumerFactory(IKafkaProperties kafkaProperties) {
        return (consumerType, groupId, instanceGroupId, keyDeserializer, valueDeserializer) -> this
            .buildConsumer(kafkaProperties, consumerType, groupId, instanceGroupId, keyDeserializer, valueDeserializer);
    }

    private class KafkaConsumerPropertiesAndSettings<K, V> {
        protected final IKafkaProperties                 kafkaProperties;
        protected final KafkaConsumerProperties          kafkaConsumerProperties;
        protected final Properties                       settings;
        protected final String                           groupId;
        protected final String                           instanceGroupId;
        protected final Class<? extends Deserializer<K>> keyDeserializer;
        protected final Class<? extends Deserializer<V>> valueDeserializer;

        public KafkaConsumerPropertiesAndSettings(
            IKafkaProperties kafkaProperties,
            KafkaConsumerProperties kafkaConsumerProperties,
            String groupId,
            String instanceGroupId,
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer
        ) {
            this.kafkaConsumerProperties = kafkaConsumerProperties;
            this.settings = new Properties();
            this.kafkaProperties = kafkaProperties;
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
            this.groupId = groupId;
            this.instanceGroupId = instanceGroupId;
        }

        public KafkaConsumerPropertiesAndSettings<K, V> build() {
            this.settings.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                this.kafkaProperties.getBootStrapServers()
                    .stream()
                    .collect(Collectors.joining(",")));
            this.settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer);
            this.settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer);
            this.settings.put(ConsumerConfig.CLIENT_ID_CONFIG, this.kafkaProperties.getApplicationId());
            this.settings.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                this.groupId == null ? this.kafkaConsumerProperties.groupId() : this.groupId);
            this.settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.settings
                .put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, this.kafkaConsumerProperties.maxPollIntervallMs());
            this.settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.kafkaConsumerProperties.maxPollRecords());
            this.settings.put(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                this.instanceGroupId == null ? this.kafkaConsumerProperties.instanceGroupId() : this.instanceGroupId);
            this.settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
            this.settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.kafkaConsumerProperties.sessionTimeout());
            this.settings.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, this.kafkaConsumerProperties.fetchMaxBytes());
            this.settings.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, this.kafkaConsumerProperties.fetchMaxWaitMs());
            this.settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            this.settings.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.toString()
                    .toLowerCase(Locale.ROOT));

            this.settings.put("sasl.kerberos.service.name", "kafka");
            return this;
        }

        public Properties getSettings() { return this.settings; }

    }

    private class KafkaProducerPropertiesAndSettings<K, V> {

        protected final IKafkaProperties               kafkaProperties;
        protected final KafkaProducerProperties        kafkaProducerProperties;
        protected final Properties                     settings;
        protected final Class<? extends Serializer<K>> keySerializer;
        protected final Class<? extends Serializer<V>> valueSerializer;

        public KafkaProducerPropertiesAndSettings(
            IKafkaProperties kafkaProperties,
            KafkaProducerProperties kafkaProducerProperties,
            Class<? extends Serializer<K>> keySerializer,
            Class<? extends Serializer<V>> valueSerializer
        ) {
            this.kafkaProducerProperties = kafkaProducerProperties;
            this.settings = new Properties();
            this.kafkaProperties = kafkaProperties;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public KafkaProducerPropertiesAndSettings<K, V> build() {
            this.settings.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                this.kafkaProperties.getBootStrapServers()
                    .stream()
                    .collect(Collectors.joining(",")));
            this.settings.put(ProducerConfig.CLIENT_ID_CONFIG, this.kafkaProperties.getApplicationId());
            this.settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer);
            this.settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer);
            this.settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            this.settings.put(ProducerConfig.LINGER_MS_CONFIG, this.kafkaProducerProperties.lingerInMillis());
            this.settings.put(ProducerConfig.BATCH_SIZE_CONFIG, this.kafkaProducerProperties.maxBatchSize());
            this.settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.kafkaProducerProperties.transactionId());
            this.settings.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
            this.settings.put(ProducerConfig.ACKS_CONFIG, "all");
            this.settings
                .put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, this.kafkaProducerProperties.transactionTimeout());
            this.settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
            this.settings.put("sasl.kerberos.service.name", "kafka");
            return this;
        }

    }

    protected <K, V> Consumer<K, V> buildConsumer(
        IKafkaProperties kafkaProperties,
        String consumerName,
        String groupId,
        String instanceGroupId,
        Class<? extends Deserializer<K>> keyDeserializer,
        Class<? extends Deserializer<V>> valueDeserializer
    ) {

        Properties settings = Optional.ofNullable(
            kafkaProperties.getConsumersType()
                .get(consumerName))
            .map(
                t -> new KafkaConsumerPropertiesAndSettings<K, V>(kafkaProperties,
                    t,
                    groupId,
                    instanceGroupId,
                    keyDeserializer,
                    valueDeserializer))
            .map(t -> t.build())
            .map(t -> t.getSettings())
            .orElseThrow(() -> new RuntimeException("µUnavble to find " + consumerName));

        Consumer<K, V> consumer = new KafkaConsumer<>(settings);
        return consumer;
    }

    protected <K, V> Producer<K, V> buildProducer(
        IKafkaProperties kafkaProperties,
        String producerName,
        Class<? extends Serializer<K>> keySerializer,
        Class<? extends Serializer<V>> valueSerializer
    ) {

        Properties settings = new Properties();

        Optional.ofNullable(
            kafkaProperties.getProducersType()
                .get(producerName))
            .map(t -> new KafkaProducerPropertiesAndSettings<K, V>(kafkaProperties, t, keySerializer, valueSerializer))
            .map(t -> t.build());

        Producer<K, V> producer = new KafkaProducer<>(settings);
        return producer;
    }

}