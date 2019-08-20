package com.gs.photos.serializers;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.HbaseImageThumbnail;

public class HbaseImageThumbnailSerializer implements Serializer<HbaseImageThumbnail> {
	protected final Schema schema;
	protected final DatumWriter<HbaseImageThumbnail> writer;

	protected <V extends HbaseImageThumbnail> byte[] toBytesGeneric(final HbaseImageThumbnail data,
			final Class<HbaseImageThumbnailSerializer> class1) {
		final ByteArrayOutputStream bout = new ByteArrayOutputStream();
		final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);

		try {

			try {
				writer.write(data, binEncoder);
				binEncoder.flush();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bout.toByteArray();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, HbaseImageThumbnail data) {
		return toBytesGeneric(data, HbaseImageThumbnailSerializer.class);
	}

	@Override
	public void close() {

	}

	public HbaseImageThumbnailSerializer() {
		schema = ReflectData.get().getSchema(HbaseImageThumbnail.class);
		writer = new ReflectDatumWriter<HbaseImageThumbnail>(schema);
	}

}
