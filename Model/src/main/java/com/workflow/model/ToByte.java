package com.workflow.model;

public interface ToByte<T> {
	byte[] convert(T parameter);

	T fromByte(byte[] parameter, int offset, int length);
}
