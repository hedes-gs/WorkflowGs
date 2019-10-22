package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.HbaseData;

public class HbaseDataDeserializer extends AbstractModelSerializerAndDeserializer<HbaseData>
		implements Deserializer<HbaseData> {

	@Override
	public HbaseData deserialize(String topic, byte[] data) {
		return this.fromBytesGeneric(data);
	}

	public HbaseDataDeserializer() {
		super();
	}

}
