package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageThumbnailKey;;

public final class HbaseImageThumbnailKeySerDe implements Serde<HbaseImageThumbnailKey> {

    protected HbaseImageThumbnailKeySerializer   hbaseImageThumbnailSerializer   = new HbaseImageThumbnailKeySerializer();
    protected HbaseImageThumbnailKeyDeserializer hbaseImageThumbnailDeserializer = new HbaseImageThumbnailKeyDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {}

    @Override
    public Serializer<HbaseImageThumbnailKey> serializer() { return this.hbaseImageThumbnailSerializer; }

    @Override
    public Deserializer<HbaseImageThumbnailKey> deserializer() { return this.hbaseImageThumbnailDeserializer; }

}