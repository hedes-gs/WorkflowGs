package com.gs.photo.workflow;

import java.util.Locale;
import java.util.Properties;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.photos.serializers.ExchangedDataSerializer;
import com.workflow.model.ExchangedTiffData;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
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
	public final static String              HBASE_IMAGE_EXIF_DATA_DESERIALIZER           = com.gs.photos.serializers.HbaseExifDataDeserializer.class
			.getName();
	public final static String              HBASE_IMAGE_EXIF_DATA_SERIALIZER             = com.gs.photos.serializers.HbaseExifDataSerializer.class
			.getName();
	public final static String              HBASE_IMAGE_EXIF_DATA_OF_IMAGES_DESERIALIZER = com.gs.photos.serializers.HbaseExifDataOfImagesDeserializer.class
			.getName();
	public final static String              HBASE_IMAGE_EXIF_DATA_OF_IMAGES_SERIALIZER   = com.gs.photos.serializers.HbaseExifDataOfImagesSerializer.class
			.getName();
	public final static String              HBASE_DATA_OF_IMAGES_DESERIALIZER            = com.gs.photos.serializers.HbaseDataDeserializer.class
			.getName();
	public final static String              HBASE_DATA_OF_IMAGES_SERIALIZER              = com.gs.photos.serializers.HbaseDataSerializer.class
			.getName();
	public final static String              CACHE_NAME                                   = "start-raw-files";
	@Value("${application.id}")
	protected String                        applicationId;

	@Value("${application.kafkastreams.id}")
	protected String                        applicationGroupId;

	@Value("${group.id}")
	protected String                        groupId;

	@Value("${transaction.id}")
	protected String                        transactionId;

	@Value("${bootstrap.servers}")
	protected String                        bootstrapServers;

	@Value("${kafkaStreamDir.dir}")
	protected String                        kafkaStreamDir;

	@Value("${topic.inputImageNameTopic}")
	protected String                        inputImageNameTopic;

	@Value("${topic.topicDupFilteredFile}")
	protected String                        topicDupFilteredFile;

	@Value("${topic.topicExif}")
	protected String                        topicExif;

	@Value("${topic.topicTransformedThumb}")
	protected String                        topicTransformedThumb;

	@Value("${topic.topicImageDataToPersist}")
	protected String                        topicImageDataToPersist;

	@Value("${topic.topicExifImageDataToPersist}")
	protected String                        topicExifImageDataToPersist;

	@Value("${copy.group.id}")
	protected String                        copyGroupId;

	@Value("${transaction.timeout}")
	protected String                        transactionTimeout;

	@Value("${hbase.master}")
	protected String                        hbaseMaster;

	@Value("${zookeeper.hosts}")
	protected String                        zookeeperHosts;

	@Value("${zookeeper.port}")
	protected int                           zookeeperPort;

	@Value("${application.gs.principal}")
	protected String                        principal;

	@Value("${application.gs.keytab}")
	protected String                        keytab;

	@Bean
	@ConditionalOnProperty(name = "producer.string.string", havingValue = "true")
	public Producer<String, String> producerForPublishingOnImageTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		settings.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		Producer<String, String> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean
	@ConditionalOnProperty(name = "producer.string.string", havingValue = "true")
	public Producer<String, String> producerForPublishingOnStringTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		settings.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		AbstractApplicationConfig.LOGGER.info("creating producer string string with config {} ",
				settings.toString());
		Producer<String, String> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean("producerForPublishingInModeTransactionalOnStringTopic")
	@ConditionalOnProperty(name = "producer.string.string.transactional", havingValue = "true")
	public Producer<String, String> producerForPublishingInModeTransactionalOnStringTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
				this.transactionId);
		settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
				"true");
		settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
				this.transactionTimeout);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		Producer<String, String> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean("producerForPublishingInModeTransactionalOnLongTopic")
	@ConditionalOnProperty(name = "producer.string.long.transactional", havingValue = "true")
	public Producer<String, String> producerForPublishingInModeTransactionalOnLongTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
				this.transactionId);
		settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
				"true");
		settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
				this.transactionTimeout);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				AbstractApplicationConfig.KAFKA_LONG_SERIALIZER);
		Producer<String, String> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean(name = "producerForPublishingOnExifTopic")
	@ConditionalOnProperty(name = "producer.string.exchangedData", havingValue = "true")
	public Producer<String, ExchangedTiffData> producerForPublishingOnExifTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("value.serializer",
				AbstractApplicationConfig.KAFKA_EXCHANGED_DATA_SERIALIZER);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		Producer<String, ExchangedTiffData> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean(name = "producerForPublishingOnJpegImageTopic")
	@ConditionalOnProperty(name = "producer.string.exchangedData", havingValue = "true")
	public Producer<String, byte[]> producerForPublishingOnJpegImageTopic() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("value.serializer",
				AbstractApplicationConfig.KAFKA_BYTE_SERIALIZER);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		Producer<String, byte[]> producer = new KafkaProducer<>(settings);
		return producer;
	}

	@Bean(name = "consumerForTopicWithStringKey")
	@ConditionalOnProperty(name = "consumer.string.string", havingValue = "true")
	public Consumer<String, String> consumerForTopicWithStringKey() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("key.deserializer",
				AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put("value.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("value.deserializer",
				AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put(ConsumerConfig.CLIENT_ID_CONFIG,
				this.applicationId);
		settings.put(ConsumerConfig.GROUP_ID_CONFIG,
				this.groupId);
		settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
				5);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		Consumer<String, String> producer = new KafkaConsumer<>(settings);
		return producer;
	}

	@Bean(name = "consumerForTransactionalCopyForTopicWithStringKey")
	@ConditionalOnProperty(name = "consumer.string.string.transactional", havingValue = "true")
	public Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		settings.put("key.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("key.deserializer",
				AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put("value.serializer",
				AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
		settings.put("value.deserializer",
				AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put(ConsumerConfig.CLIENT_ID_CONFIG,
				"tr-" + this.applicationId);
		settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		settings.put(ConsumerConfig.GROUP_ID_CONFIG,
				this.copyGroupId);
		settings.put("enable.auto.commit",
				"false");
		settings.put("isolation.level",
				"read_committed");
		settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
				5);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				"SASL_PLAINTEXT");
		settings.put("sasl.kerberos.service.name",
				"kafka");
		Consumer<String, String> producer = new KafkaConsumer<>(settings);
		return producer;
	}

	protected Properties buildConsumerCommonKafkaProperties() {
		Properties settings = new Properties();
		settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);

		settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		settings.put(ConsumerConfig.GROUP_ID_CONFIG,
				this.copyGroupId);
		settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				"false");
		settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
				IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
		settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
				5);
		settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				SecurityProtocol.SASL_PLAINTEXT.name);
		settings.put("sasl.kerberos.service.name",
				"kafka");
		return settings;
	}

	@Bean(name = "threadPoolTaskExecutor")
	public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
		final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

		threadPoolTaskExecutor.setCorePoolSize(4);
		threadPoolTaskExecutor.setMaxPoolSize(10);
		threadPoolTaskExecutor.setThreadNamePrefix("wf-task-executor");
		threadPoolTaskExecutor.initialize();
		return threadPoolTaskExecutor;
	}

	@Bean
	@ConditionalOnProperty(name = "ignite.is.used", havingValue = "true")
	public IgniteCache<String, byte[]> clientCache() {
		try (
				AbstractApplicationContext ctx = new FileSystemXmlApplicationContext(
					AbstractApplicationConfig.CONFIG_CLUSTER_CLIENT_XML)) {
			ctx.registerShutdownHook();
			Ignite ignite = (Ignite) ctx.getBean(AbstractApplicationConfig.IGNITE_SPRING_BEAN);
			return ignite.getOrCreateCache(AbstractApplicationConfig.CACHE_NAME);
		}
	}

}