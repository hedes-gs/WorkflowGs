package com.gs.photo.common.workflow;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

public interface IKafkaProducerFactory<K, V> {
    public Producer<K, V> get(
        String producerName,
        Class<? extends Serializer<K>> kafkaKeySerializer,
        Class<? extends Serializer<V>> kafkaMultipleSerializer
    );
}
