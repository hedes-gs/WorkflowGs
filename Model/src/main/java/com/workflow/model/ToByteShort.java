package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteShort extends ToByte<Short> {

	@Override
	public default byte[] convert(Short p) {
		byte[] retValue = new byte[2];
		Bytes.putShort(retValue,
			0,
			p);
		return retValue;
	}

	@Override
	public default Short fromByte(byte[] parameter, int offset, int length) {
		return Bytes.toShort(parameter,
			offset);
	}
}
