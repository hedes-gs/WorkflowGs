package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.HbaseExifData;

public class HbaseExifDataDeserializer extends AbstractModelSerializerAndDeserializer<HbaseExifData>
		implements Deserializer<HbaseExifData> {

	@Override
	public HbaseExifData deserialize(String topic, byte[] data) {
		return this.fromBytesGeneric(data);
	}

	public HbaseExifDataDeserializer() {
		super();
	}

}
