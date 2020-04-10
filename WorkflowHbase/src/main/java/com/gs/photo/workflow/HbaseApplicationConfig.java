package com.gs.photo.workflow;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseImageThumbnail;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")

public class HbaseApplicationConfig extends AbstractApplicationConfig {
	private static final String HBASE_MASTER_KERBEROS_PRINCIPAL       = "hbase.master.kerberos.principal";
	private static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
	private static final String HBASE_RPC_PROTECTION                  = "hbase.rpc.protection";
	private static final String HBASE_SECURITY_AUTHENTICATION         = "hbase.security.authentication";
	private static final String HADOOP_SECURITY_AUTHENTICATION        = "hadoop.security.authentication";
	private static final String CONSUMER_IMAGE                        = "consumer-image";
	private static final String CONSUMER_EXIF                         = "consumer-exif";
	private static final String CONSUMER_EXIF_DATA_OF_IMAGES          = "consumer-exif-data-of-images";

	private static Logger       LOGGER                                = LogManager
		.getLogger(HbaseApplicationConfig.class);

	@Bean
	protected org.apache.hadoop.conf.Configuration hbaseConfiguration(
		@Value("${zookeeper.hosts}") String zookeeperHosts,
		@Value("${zookeeper.port}") int zookeeperPort
	) {

		org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
		hBaseConfig.setInt("timeout",
			120000);
		hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM,
			zookeeperHosts);
		hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
			zookeeperPort);
		hBaseConfig.set(HbaseApplicationConfig.HADOOP_SECURITY_AUTHENTICATION,
			"kerberos");
		hBaseConfig.set(HbaseApplicationConfig.HBASE_SECURITY_AUTHENTICATION,
			"kerberos");
		hBaseConfig.set(HConstants.CLUSTER_DISTRIBUTED,
			"true");
		hBaseConfig.set(HbaseApplicationConfig.HBASE_RPC_PROTECTION,
			"authentication");
		hBaseConfig.set(HbaseApplicationConfig.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL,
			"hbase/_HOST@GS.COM");
		hBaseConfig.set(HbaseApplicationConfig.HBASE_MASTER_KERBEROS_PRINCIPAL,
			"hbase/_HOST@GS.COM");

		return hBaseConfig;
	}

	@Bean
	public Connection hbaseConnection(
		@Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
		@Value("${application.gs.principal}") String principal,
		@Value("${application.gs.keytab}") String keytab
	) {
		HbaseApplicationConfig.LOGGER.info("creating the hbase connection");
		UserGroupInformation.setConfiguration(hbaseConfiguration);
		try {
			UserGroupInformation.loginUserFromKeytab(principal,
				keytab);
		} catch (IOException e) {
			HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection",
				e);
			throw new RuntimeException(e);
		}
		PrivilegedAction<Connection> action = () -> {
			try {
				return ConnectionFactory.createConnection(hbaseConfiguration);
			} catch (IOException e) {
				HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection",
					e);
			}
			return null;
		};
		try {
			return UserGroupInformation.getCurrentUser().doAs(action);
		} catch (IOException e) {
			HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection",
				e);
			throw new RuntimeException(e);
		}
	}

	@Bean(name = "consumerToRecordExifDataOfImages")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseData> consumerToRecordExifDataOfImages(
		@Qualifier("consumerCommonKafkaProperties") Properties consumerCommonKafkaProperties
	) {
		Properties settings = new Properties();
		settings.putAll(consumerCommonKafkaProperties);
		settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.HBASE_IMAGE_EXIF_DATA_OF_IMAGES_DESERIALIZER);
		settings.put(ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + this.applicationId + "-" + HbaseApplicationConfig.CONSUMER_EXIF_DATA_OF_IMAGES);

		Consumer<String, HbaseData> consumer = new KafkaConsumer<>(settings);
		return consumer;
	}

	@Bean(name = "consumerForRecordingExifDataFromTopic")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseExifData> consumerForRecordingExifDataFromTopic(
		@Qualifier("consumerCommonKafkaProperties") Properties consumerCommonKafkaProperties
	) {
		Properties settings = new Properties();
		settings.putAll(consumerCommonKafkaProperties);
		settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
		settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.HBASE_IMAGE_EXIF_DATA_DESERIALIZER);
		settings.put(ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + this.applicationId + "-" + HbaseApplicationConfig.CONSUMER_EXIF);

		Consumer<String, HbaseExifData> consumer = new KafkaConsumer<>(settings);
		return consumer;

	}

	@Bean(name = "consumerForRecordingImageFromTopic")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic(
		@Qualifier("consumerCommonKafkaProperties") Properties consumerCommonKafkaProperties
	) {
		Properties settings = new Properties();
		settings.putAll(consumerCommonKafkaProperties);

		settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);

		settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			AbstractApplicationConfig.HBASE_IMAGE_THUMBNAIL_DESERIALIZER);
		settings.put(ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + this.applicationId + "-" + HbaseApplicationConfig.CONSUMER_IMAGE);
		Consumer<String, HbaseImageThumbnail> consumer = new KafkaConsumer<>(settings);
		return consumer;

	}
}
