package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.HbaseExifDataOfImages;

public class HbaseExifDataOfImagesDeserializer extends AbstractModelSerializerAndDeserializer<HbaseExifDataOfImages>
		implements Deserializer<HbaseExifDataOfImages> {

	@Override
	public HbaseExifDataOfImages deserialize(String topic, byte[] data) {
		return this.fromBytesGeneric(topic, data);
	}

	public HbaseExifDataOfImagesDeserializer() {
		super();
	}

}
