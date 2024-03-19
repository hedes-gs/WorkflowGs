package com.gs.photo.common.workflow;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

public interface IKafkaConsumerFactory<K, V> {
    default public Consumer<K, V> get(
        String consumerType,
        Class<? extends Deserializer<K>> kafkaStringDeserializer,
        Class<? extends Deserializer<V>> kafkaFileToProcessDeserializer
    ) {
        return this.get(consumerType, null, null, kafkaStringDeserializer, kafkaFileToProcessDeserializer);
    };

    public Consumer<K, V> get(
        String consumerType,
        String groupId,
        String instanceGroupId,
        Class<? extends Deserializer<K>> kafkaStringDeserializer,
        Class<? extends Deserializer<V>> kafkaFileToProcessDeserializer
    );
}
