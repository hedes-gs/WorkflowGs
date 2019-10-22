package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteIntArray extends ToByte<int[]> {
	@Override
	public default byte[] convert(int[] p) {
		byte[] retValue = new byte[4 * p.length];
		for (int k = 0; k < p.length; k++) {
			Bytes.putInt(
				retValue,
				4 * k,
				p[k]);
		}
		return retValue;
	}

	@Override
	default int[] fromByte(byte[] parameter, int offset, int length) {
		int[] retValue = new int[parameter.length / 4];
		for (int k = 0; k < retValue.length; k++) {
			retValue[k] = Bytes.toInt(
				parameter,
				4 * k);
		}
		return retValue;
	}

}