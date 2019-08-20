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

import com.workflow.model.ExchangedTiffData;

public class ExchangedDataSerializer implements Serializer<ExchangedTiffData> {
	protected Schema schema;
	protected DatumWriter<ExchangedTiffData> writer;

	protected <V extends ExchangedTiffData> byte[] toBytesGeneric(final ExchangedTiffData data,
			final Class<ExchangedDataSerializer> class1) {
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
	public byte[] serialize(String topic, ExchangedTiffData data) {
		return toBytesGeneric(data, ExchangedDataSerializer.class);
	}

	@Override
	public void close() {

	}

	public ExchangedDataSerializer() {
		schema = ReflectData.get().getSchema(ExchangedTiffData.class);
		writer = new ReflectDatumWriter<ExchangedTiffData>(schema);
	}

}
