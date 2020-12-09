package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageThumbnail;

public class HbaseImageThumbnailSerializer extends AbstractModelSerializerAndDeserializer<HbaseImageThumbnail>
		implements Serializer<HbaseImageThumbnail> {

	@Override
	public byte[] serialize(String topic, HbaseImageThumbnail data) {
		return this.toBytesGeneric(topic, data);
	}

	public HbaseImageThumbnailSerializer() {
		super();
	}

}
