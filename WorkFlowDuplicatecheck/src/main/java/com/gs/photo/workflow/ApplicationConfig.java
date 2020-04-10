package com.gs.photo.workflow;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
public class ApplicationConfig extends AbstractApplicationConfig {

	@Bean(name = "kafkaStreamProperties")
	public Properties kafkaStreamProperties(
		@Value("${bootstrap.servers}") String bootstrapServers,
		@Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
		@Value("${application.id}") String applicationId
	) {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG,
			applicationId + "-duplicate-streams");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
			bootstrapServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
			Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
			Serdes.String().getClass());
		config.put(StreamsConfig.STATE_DIR_CONFIG,
			kafkaStreamDir);
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
			StreamsConfig.EXACTLY_ONCE);
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
			"0");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
			"earliest");
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
			0);
		config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
			"SASL_PLAINTEXT");
		config.put("sasl.kerberos.service.name",
			"kafka");
		return config;
	}
}
