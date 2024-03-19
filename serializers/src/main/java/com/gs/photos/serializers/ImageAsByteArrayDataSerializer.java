package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ImageAsByteArray;

public class ImageAsByteArrayDataSerializer extends AbstractModelSerializerAndDeserializer<ImageAsByteArray>
    implements Serializer<ImageAsByteArray> {

    @Override
    public byte[] serialize(String topic, ImageAsByteArray data) { return this.toBytesGeneric(topic, data); }

    public ImageAsByteArrayDataSerializer() { super(); }

}
