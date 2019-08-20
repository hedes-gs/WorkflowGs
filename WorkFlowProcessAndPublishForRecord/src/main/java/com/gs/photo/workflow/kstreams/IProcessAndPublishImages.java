package com.gs.photo.workflow.kstreams;

import org.apache.kafka.streams.KafkaStreams;

public interface IProcessAndPublishImages {
	public KafkaStreams buildKafkaStreamsTopology();
}
