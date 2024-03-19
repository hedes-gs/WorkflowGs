package com.gsphotos.storms;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.gs.photos.serializers.ExchangedDataSerializer;
import com.workflow.model.ExchangedTiffData;

public class ApplicationConfig {

	protected String kafkaServers;

	public Producer<String, String> producerForPublishingOnImageTopic() {
		Properties settings = new Properties();
		settings.put(
			CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
			kafkaServers);
		settings.put(
			"key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(
			"value.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(settings);
		return producer;
	}

	public Producer<String, ExchangedTiffData> producerForPublishingOnExifTopic() {
		Properties settings = new Properties();
		settings.put(
			CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
			kafkaServers);
		settings.put(
			"key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(
			"value.serializer",
			ExchangedDataSerializer.class.getName());
		Producer<String, ExchangedTiffData> producer = new KafkaProducer<>(settings);
		return producer;
	}
}