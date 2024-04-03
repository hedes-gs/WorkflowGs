package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageAsByteArray;

public class ImageAsByteArrayDataSerializer extends AbstractModelSerializerAndDeserializer<HbaseImageAsByteArray>
    implements Serializer<HbaseImageAsByteArray> {

    @Override
    public byte[] serialize(String topic, HbaseImageAsByteArray data) { return this.toBytesGeneric(topic, data); }

    public ImageAsByteArrayDataSerializer() { super(); }

}
