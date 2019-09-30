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

import com.workflow.model.HbaseExifDataOfImages;

public class HbaseExifDataOfImagesDeserializer implements Deserializer<HbaseExifDataOfImages> {

	protected Schema schema;
	protected DatumReader<HbaseExifDataOfImages> reader;

	@SuppressWarnings("unchecked")
	protected HbaseExifDataOfImages fromBytesGeneric(byte[] t) {
		HbaseExifDataOfImages retValue = null;
		if (t != null & t.length > 0) {
			try {
				final ByteArrayInputStream bais = new ByteArrayInputStream(t);
				final BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(
					bais,
					null);
				Object cv = null;
				try {
					cv = reader.read(
						null,
						binDecoder);
					retValue = (HbaseExifDataOfImages) cv;
				} catch (final Exception e) {
					e.printStackTrace();
					System.out.println(
						"HbaseImageThumbnail.class.loader " + HbaseExifDataOfImages.class.getClassLoader());

					System.out.println(
						"Unable to convert " + cv.getClass().getClassLoader());
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
	public HbaseExifDataOfImages deserialize(String topic, byte[] data) {
		return fromBytesGeneric(
			data);
	}

	@Override
	public void close() {

	}

	public HbaseExifDataOfImagesDeserializer() {
		schema = ReflectData.get().getSchema(
			HbaseExifDataOfImages.class);
		reader = new ReflectDatumReader<HbaseExifDataOfImages>(schema);
	}

}
