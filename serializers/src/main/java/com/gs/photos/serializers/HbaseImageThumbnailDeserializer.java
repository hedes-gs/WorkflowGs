package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.HbaseImageThumbnail;

public class HbaseImageThumbnailDeserializer extends AbstractModelSerializerAndDeserializer<HbaseImageThumbnail>
		implements Deserializer<HbaseImageThumbnail> {

	@Override
	public HbaseImageThumbnail deserialize(String topic, byte[] data) {
		return this.fromBytesGeneric(topic, data);
	}

	public HbaseImageThumbnailDeserializer() {
		super();
	}

}
