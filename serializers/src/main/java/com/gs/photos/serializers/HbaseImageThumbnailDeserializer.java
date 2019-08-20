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

import com.workflow.model.HbaseImageThumbnail;

public class HbaseImageThumbnailDeserializer implements Deserializer<HbaseImageThumbnail> {

	protected Schema schema;
	protected DatumReader<HbaseImageThumbnail> reader;

	@SuppressWarnings("unchecked")
	protected HbaseImageThumbnail fromBytesGeneric(byte[] t) {
		HbaseImageThumbnail retValue = null;
		if (t != null & t.length > 0) {
			try {
				final ByteArrayInputStream bais = new ByteArrayInputStream(t);
				final BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bais, null);
				Object cv = null;
				try {
					cv = reader.read(null, binDecoder);
					retValue = (HbaseImageThumbnail) cv;
				} catch (final Exception e) {
					e.printStackTrace();
					System.out
							.println("HbaseImageThumbnail.class.loader " + HbaseImageThumbnail.class.getClassLoader());

					System.out.println("Unable to convert " + cv.getClass().getClassLoader());
					throw new RuntimeException(e);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return retValue;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public HbaseImageThumbnail deserialize(String topic, byte[] data) {
		return fromBytesGeneric(data);
	}

	@Override
	public void close() {

	}

	public HbaseImageThumbnailDeserializer() {
		schema = ReflectData.get().getSchema(HbaseImageThumbnail.class);
		reader = new ReflectDatumReader<HbaseImageThumbnail>(schema);
	}

}
