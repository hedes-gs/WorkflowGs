package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.CollectionOfExchangedTiffData;

public class CollectionOfExchangedDataSerializer
    extends AbstractModelSerializerAndDeserializer<CollectionOfExchangedTiffData>
    implements Serializer<CollectionOfExchangedTiffData> {

    public CollectionOfExchangedDataSerializer() { super(); }

    @Override
    public byte[] serialize(String topic, CollectionOfExchangedTiffData data) { return super.toBytesGeneric(topic, data); }

}
