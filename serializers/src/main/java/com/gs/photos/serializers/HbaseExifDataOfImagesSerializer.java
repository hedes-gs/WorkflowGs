package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseExifDataOfImages;

public class HbaseExifDataOfImagesSerializer extends AbstractModelSerializerAndDeserializer<HbaseExifDataOfImages>
		implements Serializer<HbaseExifDataOfImages> {

	@Override
	public byte[] serialize(String topic, HbaseExifDataOfImages data) {
		return this.toBytesGeneric(topic, data);
	}

	HbaseExifDataOfImagesSerializer() {
		super();
	}

}
