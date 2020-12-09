package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.ExchangedTiffData;

public class ExchangedDataDeserializer extends AbstractModelSerializerAndDeserializer<ExchangedTiffData>
		implements Deserializer<ExchangedTiffData> {

	@Override
	public ExchangedTiffData deserialize(String topic, byte[] data) {
		return this.fromBytesGeneric(topic, data);
	}

	public ExchangedDataDeserializer() {
		super();
	}

}
