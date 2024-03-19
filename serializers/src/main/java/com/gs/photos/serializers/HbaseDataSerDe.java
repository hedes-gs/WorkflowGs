package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseData;;

public final class HbaseDataSerDe implements Serde<HbaseData> {

	protected HbaseDataSerializer   hbaseExifDataSerializer   = new HbaseDataSerializer();
	protected HbaseDataDeserializer hbaseExifDataDeserializer = new HbaseDataDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<HbaseData> serializer() {
		return this.hbaseExifDataSerializer;
	}

	@Override
	public Deserializer<HbaseData> deserializer() {
		return this.hbaseExifDataDeserializer;
	}

}