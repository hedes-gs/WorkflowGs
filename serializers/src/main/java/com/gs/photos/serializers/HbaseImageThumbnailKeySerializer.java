package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageThumbnailKey;

public class HbaseImageThumbnailKeySerializer extends AbstractModelSerializerAndDeserializer<HbaseImageThumbnailKey>
    implements Serializer<HbaseImageThumbnailKey> {

    @Override
    public byte[] serialize(String topic, HbaseImageThumbnailKey data) { return this.toBytesGeneric(topic, data); }

    public HbaseImageThumbnailKeySerializer() { super(); }

}
