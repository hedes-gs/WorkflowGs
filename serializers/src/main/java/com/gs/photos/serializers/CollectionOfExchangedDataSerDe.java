package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.CollectionOfExchangedTiffData;

public final class CollectionOfExchangedDataSerDe implements Serde<CollectionOfExchangedTiffData> {

    protected CollectionOfExchangedDataSerializer   exchangedDataSerializer   = new CollectionOfExchangedDataSerializer();
    protected CollectionOfExchangedDataDeserializer exchangedDataDeserializer = new CollectionOfExchangedDataDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {}

    @Override
    public Serializer<CollectionOfExchangedTiffData> serializer() { return this.exchangedDataSerializer; }

    @Override
    public Deserializer<CollectionOfExchangedTiffData> deserializer() { return this.exchangedDataDeserializer; }

}