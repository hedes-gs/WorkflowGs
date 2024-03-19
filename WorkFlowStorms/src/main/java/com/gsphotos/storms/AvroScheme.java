package com.gsphotos.storms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import com.workflow.model.ExchangedTiffData;

public class AvroScheme implements Scheme {

	private static final long serialVersionUID = 1L;

	public static <V extends ExchangedTiffData> byte[] toBytesGeneric(final V t,
			final Class<? extends ExchangedTiffData> class1) {
		final ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			final Schema schema = ReflectData.get().getSchema(class1);
			final DatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
			final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
			try {
				writer.write(t, binEncoder);
				binEncoder.flush();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bout.toByteArray();
	}

	public static <V extends ExchangedTiffData> V fromBytesGeneric(byte[] t,
			final Class<? extends ExchangedTiffData> class1) {
		V retValue = null;
		try {
			final ByteArrayInputStream bais = new ByteArrayInputStream(t);
			final Schema schema = ReflectData.get().getSchema(class1);
			final DatumReader<ExchangedTiffData> reader = new ReflectDatumReader<ExchangedTiffData>(schema);
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

	private static byte[] getByteArrayFromByteBuffer(ByteBuffer byteBuffer) {
		byte[] bytesArray = new byte[byteBuffer.remaining()];
		byteBuffer.get(bytesArray, 0, bytesArray.length);
		return bytesArray;
	}

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		return Arrays.asList(fromBytesGeneric(getByteArrayFromByteBuffer(ser), ExchangedTiffData.class));
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("bytes");
	}

}
