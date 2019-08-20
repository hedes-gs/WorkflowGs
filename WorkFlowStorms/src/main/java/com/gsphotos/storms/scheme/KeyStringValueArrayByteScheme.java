package com.gsphotos.storms.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.common.utils.Utils;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;

public class KeyStringValueArrayByteScheme implements KeyValueScheme {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Fields getOutputFields() {
		return new Fields("KEY", "VALUE");
	}

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		return null;
	}

	@Override
	public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
		String keyString = StringScheme.deserializeString(
			key);
		byte[] valueAsByte = Utils.toArray(
			value);
		return new Values(
			ImmutableMap.of(
				keyString,
				valueAsByte));
	}
}
