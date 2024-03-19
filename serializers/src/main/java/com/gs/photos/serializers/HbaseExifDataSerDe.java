package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseExifData;;

public final class HbaseExifDataSerDe implements Serde<HbaseExifData> {

	protected HbaseExifDataSerializer hbaseExifDataSerializer = new HbaseExifDataSerializer();
	protected HbaseExifDataDeserializer hbaseExifDataDeserializer = new HbaseExifDataDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<HbaseExifData> serializer() {
		return hbaseExifDataSerializer;
	}

	@Override
	public Deserializer<HbaseExifData> deserializer() {
		return hbaseExifDataDeserializer;
	}

}