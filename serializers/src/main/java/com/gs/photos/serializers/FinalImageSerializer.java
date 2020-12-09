package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.storm.FinalImage;

public class FinalImageSerializer extends AbstractModelSerializerAndDeserializer<FinalImage>
		implements Serializer<FinalImage> {

	@Override
	public byte[] serialize(String topic, FinalImage data) {
		return super.toBytesGeneric(topic, data);
	}

	public FinalImageSerializer() {
		super();
	}

}
