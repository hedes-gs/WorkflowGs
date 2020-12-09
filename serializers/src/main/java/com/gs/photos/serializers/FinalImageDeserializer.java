package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.storm.FinalImage;

public class FinalImageDeserializer extends AbstractModelSerializerAndDeserializer<FinalImage>
		implements Deserializer<FinalImage> {

	public FinalImageDeserializer() {
		super();
	}

	@Override
	public FinalImage deserialize(String topic, byte[] t) {
		return super.fromBytesGeneric(topic, t);
	}

}
