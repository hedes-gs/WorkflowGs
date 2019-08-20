package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ExchangedTiffData;

public final class ExchangedDataSerDe implements Serde<ExchangedTiffData> {

	protected ExchangedDataSerializer exchangedDataSerializer = new ExchangedDataSerializer();
	protected ExchangedDataDeserializer exchangedDataDeserializer = new ExchangedDataDeserializer();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<ExchangedTiffData> serializer() {
		return exchangedDataSerializer;
	}

	@Override
	public Deserializer<ExchangedTiffData> deserializer() {
		return exchangedDataDeserializer;
	}

}