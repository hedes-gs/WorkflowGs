package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteShortArray extends ToByte<short[]> {
	@Override
	public default byte[] convert(short[] p) {
		byte[] retValue = new byte[2 * p.length];
		for (int k = 0; k < p.length; k++) {
			Bytes.putShort(
				retValue,
				2 * k,
				p[k]);
		}
		return retValue;
	}

	@Override
	default short[] fromByte(byte[] parameter, int offset, int length) {
		short[] retValue = new short[parameter.length / 4];
		for (int k = 0; k < retValue.length; k++) {
			retValue[k] = Bytes.toShort(
				parameter,
				2 * k);
		}
		return retValue;
	}

}