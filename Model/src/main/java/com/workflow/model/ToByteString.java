package com.workflow.model;

public interface ToByteString extends ToByte<String> {

	@Override
	public default byte[] convert(String p) {
		return p.getBytes();
	}

	@Override
	public default String fromByte(byte[] parameter, int offset, int length) {
		return new String(parameter, offset, length).trim();
	}

}
