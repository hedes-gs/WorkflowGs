package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ExchangedTiffData;

public class ExchangedDataSerializer extends AbstractModelSerializerAndDeserializer<ExchangedTiffData>
		implements Serializer<ExchangedTiffData> {

	@Override
	public byte[] serialize(String topic, ExchangedTiffData data) {
		return this.toBytesGeneric(topic, data);
	}

	public ExchangedDataSerializer() {
		super();
	}

}
