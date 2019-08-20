package com.gs.photo.workflow;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.kstreams.IProcessAndPublishImages;

@Component
public class WorkflowPhotoScheduler {

	@Autowired
	protected IProcessAndPublishImages processAndPublishImages;

	@PostConstruct
	public void init() {
		KafkaStreams kafkaStreams = processAndPublishImages.buildKafkaStreamsTopology();
		kafkaStreams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
