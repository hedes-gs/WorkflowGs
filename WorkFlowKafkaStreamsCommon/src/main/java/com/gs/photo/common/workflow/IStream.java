package com.gs.photo.common.workflow;

import org.apache.kafka.streams.Topology;

public interface IStream {
    public Topology buildKafkaStreamsTopology();

}
