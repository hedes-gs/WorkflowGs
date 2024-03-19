package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageThumbnail;;

public final class HbaseImageThumbnailSerDe implements Serde<HbaseImageThumbnail> {

	protected HbaseImageThumbnailSerializer hbaseImageThumbnailSerializer = new HbaseImageThumbnailSerializer();
	protected HbaseImageThumbnailDeserializer hbaseImageThumbnailDeserializer = new HbaseImageThumbnailDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<HbaseImageThumbnail> serializer() {
		return hbaseImageThumbnailSerializer;
	}

	@Override
	public Deserializer<HbaseImageThumbnail> deserializer() {
		return hbaseImageThumbnailDeserializer;
	}

}