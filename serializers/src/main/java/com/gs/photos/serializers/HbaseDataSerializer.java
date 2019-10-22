package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseData;

public class HbaseDataSerializer extends AbstractModelSerializerAndDeserializer<HbaseData>
		implements Serializer<HbaseData> {
	@Override
	public byte[] serialize(String topic, HbaseData data) {
		return this.toBytesGeneric(data);
	}

	public HbaseDataSerializer() {
		super();
	}

}
