package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseData;

public class HbaseGenericSerializerDeserializer<T extends HbaseData> extends AbstractModelSerializerAndDeserializer<T>
    implements Deserializer<T>, Serializer<T> {

    public HbaseGenericSerializerDeserializer() { super(); }

    @Override
    public T deserialize(String topic, byte[] t) { return super.fromBytesGeneric(topic, t); }

    @Override
    public byte[] serialize(String topic, T data) { return super.toBytesGeneric(topic, data); }

}
