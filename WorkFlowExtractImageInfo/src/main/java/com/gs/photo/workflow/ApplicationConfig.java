package com.gs.photo.workflow;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;

import com.gs.photos.serializers.HbaseExifOrImageSerializer;

public class ApplicationConfig extends AbstractApplicationConfig {

	public static final String KAFKA_EXIF_OR_IMAGE_SERIALIZER = HbaseExifOrImageSerializer.class.getName();

	@Bean("producerForTransactionPublishingOnExifOrImageTopic")
	public Producer<String, Object> producerForTransactionPublishingOnExifOrImageTopic() {
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
				ApplicationConfig.KAFKA_EXIF_OR_IMAGE_SERIALIZER);
		Producer<String, Object> producer = new KafkaProducer<>(settings);
		return producer;
	}

}
