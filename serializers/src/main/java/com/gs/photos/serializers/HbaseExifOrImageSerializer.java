package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ExchangedTiffData;

public class HbaseExifOrImageSerializer implements Serializer<Object> {

	protected ExchangedDataSerializer exchangedDataSerializer = new ExchangedDataSerializer();
	protected ByteArraySerializer     byteArraySerializer     = new ByteArraySerializer();

	@Override
	public byte[] serialize(String topic, Object data) {
		if (data instanceof byte[]) {
			return this.byteArraySerializer.serialize(topic,
					(byte[]) data);
		} else if (data instanceof ExchangedTiffData) {
			return this.exchangedDataSerializer.serialize(topic,
					(ExchangedTiffData) data);
		}
		throw new IllegalArgumentException("Unexpected object " + data);
	}

	public HbaseExifOrImageSerializer() {
		super();
	}

}
