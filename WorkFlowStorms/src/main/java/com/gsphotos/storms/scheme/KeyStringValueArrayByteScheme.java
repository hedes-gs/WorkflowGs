package com.gsphotos.storms.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.tuple.Fields;

public class KeyStringValueArrayByteScheme {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public Fields getOutputFields() {
		return new Fields("KEY", "VALUE");
	}

	public List<Object> deserialize(ByteBuffer ser) {
		return null;
	}

	public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
		/*
		 * String keyString = deserializeString( key); byte[] valueAsByte =
		 * Utils.toArray( value); return new Values( ImmutableMap.of( keyString,
		 * valueAsByte));
		 */
		return null;
	}
}
