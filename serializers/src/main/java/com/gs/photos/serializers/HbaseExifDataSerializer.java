package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseExifData;

public class HbaseExifDataSerializer extends AbstractModelSerializerAndDeserializer<HbaseExifData>
		implements Serializer<HbaseExifData> {
	@Override
	public byte[] serialize(String topic, HbaseExifData data) {
		return this.toBytesGeneric(data);
	}

	public HbaseExifDataSerializer() {
		super();
	}

}
