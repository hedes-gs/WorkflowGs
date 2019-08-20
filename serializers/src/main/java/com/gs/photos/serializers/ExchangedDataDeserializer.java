package com.gs.photos.serializers;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.ExchangedTiffData;

public class ExchangedDataDeserializer implements Deserializer<ExchangedTiffData> {

	protected final Schema schema;
	protected final DatumReader<ExchangedTiffData> reader;

	protected <V extends ExchangedTiffData> V fromBytesGeneric(byte[] t) {
		V retValue = null;
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(t);
			final BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bais, null);
			try {
				retValue = (V) reader.read(null, binDecoder);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retValue;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public ExchangedTiffData deserialize(String topic, byte[] data) {
		return fromBytesGeneric(data);
	}

	@Override
	public void close() {

	}

	public ExchangedDataDeserializer() {
		schema = ReflectData.get().getSchema(ExchangedTiffData.class);
		reader = new ReflectDatumReader<ExchangedTiffData>(schema);
	}

}
