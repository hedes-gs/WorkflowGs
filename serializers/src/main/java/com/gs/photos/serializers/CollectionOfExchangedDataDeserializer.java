package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.CollectionOfExchangedTiffData;

public class CollectionOfExchangedDataDeserializer
    extends AbstractModelSerializerAndDeserializer<CollectionOfExchangedTiffData>
    implements Deserializer<CollectionOfExchangedTiffData> {

    @Override
    public CollectionOfExchangedTiffData deserialize(String topic, byte[] data) { return this.fromBytesGeneric(topic, data); }

    public CollectionOfExchangedDataDeserializer() { super(); }

}
