package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.HbaseImageThumbnailKey;

public class HbaseImageThumbnailKeyDeserializer extends AbstractModelSerializerAndDeserializer<HbaseImageThumbnailKey>
    implements Deserializer<HbaseImageThumbnailKey> {

    @Override
    public HbaseImageThumbnailKey deserialize(String topic, byte[] data) { return this.fromBytesGeneric(topic, data); }

    public HbaseImageThumbnailKeyDeserializer() { super(); }

}
