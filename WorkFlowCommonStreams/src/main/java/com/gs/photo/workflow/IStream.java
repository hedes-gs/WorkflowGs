package com.gs.photo.workflow;

import org.apache.kafka.streams.KafkaStreams;

public interface IStream {
	public KafkaStreams buildKafkaStreamsTopology();

}
