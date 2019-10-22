package com.gs.photo.workflow;

import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkflowPhotoScheduler {

	@Autowired
	protected Properties kafkaStreamProperties;

	@Autowired
	protected Topology kafkaStreamsTopology;

	@PostConstruct
	public void init() {
		StreamsBuilder builder = new StreamsBuilder();
		final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaStreamProperties);
		kafkaStreams.start();
		Runtime.getRuntime().addShutdownHook(
			new Thread(kafkaStreams::close));
	}
}
