package com.gs.photo.common.workflow;

import org.apache.kafka.streams.KafkaStreams;

public interface IStream {
    public KafkaStreams buildKafkaStreamsTopology();

}
