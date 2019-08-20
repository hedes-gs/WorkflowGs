package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.storm.FinalImage;

public final class FinalImageSerDe implements Serde<FinalImage> {

	protected FinalImageSerializer FinalImageSerializer = new FinalImageSerializer();
	protected FinalImageDeserializer FinalImageDeserializer = new FinalImageDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<FinalImage> serializer() {
		return FinalImageSerializer;
	}

	@Override
	public Deserializer<FinalImage> deserializer() {
		return FinalImageDeserializer;
	}

}