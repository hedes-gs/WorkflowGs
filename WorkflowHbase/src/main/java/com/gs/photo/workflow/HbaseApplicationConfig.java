package com.gs.photo.workflow;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
public class HbaseApplicationConfig extends ApplicationConfig {

	private static final String CONSUMER_IMAGE = "consumer-image";
	private static final String CONSUMER_EXIF = "consumer-exif";
	private static final String CONSUMER_EXIF_DATA_OF_IMAGES = "consumer-exif-data-of-images";

	// consumerToRecordExifDataOfImages

	@Bean(name = "consumerToRecordExifDataOfImages")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseExifDataOfImages> consumerToRecordExifDataOfImages() {
		Properties settings = buildConsumerCommonKafkaProperties();

		settings.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			KAFKA_STRING_DESERIALIZER);
		settings.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			HBASE_IMAGE_EXIF_DATA_OF_IMAGES_DESERIALIZER);
		settings.put(
			ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + applicationId + "-" + CONSUMER_EXIF_DATA_OF_IMAGES);

		Consumer<String, HbaseExifDataOfImages> consumer = new KafkaConsumer<>(settings);
		return consumer;
	}

	@Bean(name = "consumerForRecordingExifDataFromTopic")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseExifData> consumerForRecordingExifDataFromTopic() {

		Properties settings = buildConsumerCommonKafkaProperties();

		settings.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			KAFKA_STRING_DESERIALIZER);
		settings.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			HBASE_IMAGE_EXIF_DATA_DESERIALIZER);
		settings.put(
			ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + applicationId + "-" + CONSUMER_EXIF);

		Consumer<String, HbaseExifData> consumer = new KafkaConsumer<>(settings);
		return consumer;

	}

	@Bean(name = "consumerForRecordingImageFromTopic")
	@ConditionalOnProperty(name = "unit-test", havingValue = "false")
	public Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic() {
		Properties settings = buildConsumerCommonKafkaProperties();

		settings.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			KAFKA_STRING_DESERIALIZER);

		settings.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			HBASE_IMAGE_THUMBNAIL_DESERIALIZER);
		settings.put(
			ConsumerConfig.CLIENT_ID_CONFIG,
			"tr-" + applicationId + "-" + CONSUMER_IMAGE);
		Consumer<String, HbaseImageThumbnail> consumer = new KafkaConsumer<>(settings);
		return consumer;

	}
}
