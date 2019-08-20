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

import com.workflow.model.storm.FinalImage;

public class FinalImageDeserializer implements Deserializer<FinalImage> {

	protected final Schema schema;
	protected final DatumReader<FinalImage> reader;

	@SuppressWarnings("unchecked")
	protected <V extends FinalImage> V fromBytesGeneric(byte[] t) {
		V retValue = null;
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(t);
			final BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bais, null);
			try {
				retValue = (V) reader.read(null, binDecoder);
			} catch (final Exception e) {
				System.err.println(this + "... unable to decode " + new String(t));
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
	public FinalImage deserialize(String topic, byte[] data) {
		return fromBytesGeneric(data);
	}

	@Override
	public void close() {

	}

	public FinalImageDeserializer() {
		schema = ReflectData.get().getSchema(FinalImage.class);
		reader = new ReflectDatumReader<FinalImage>(schema);
	}

}
