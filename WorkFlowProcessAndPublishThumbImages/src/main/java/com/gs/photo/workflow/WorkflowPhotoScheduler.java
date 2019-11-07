package com.gs.photo.workflow;

import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkflowPhotoScheduler {

	private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);

	@Autowired
	protected Properties        kafkaStreamProperties;

	@Autowired
	protected Topology          kafkaStreamsTopology;

	@PostConstruct
	public void init() {
		WorkflowPhotoScheduler.LOGGER.info("Starting kafka gttreams ");
		final KafkaStreams kafkaStreams = new KafkaStreams(this.kafkaStreamsTopology, this.kafkaStreamProperties);
		kafkaStreams.start();
		WorkflowPhotoScheduler.LOGGER.info("started kafka gttreams {} ",
			this.kafkaStreamsTopology.describe().toString());

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
