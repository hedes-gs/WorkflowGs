package com.workflow.model;

public interface ToByteIdempotent extends ToByte<byte[]> {
    @Override
    public default byte[] convert(byte[] p) { return p; }

    @Override
    default byte[] fromByte(byte[] parameter, int offset, int length) {
        byte[] retValue = new byte[length];
        System.arraycopy(parameter, offset, retValue, 0, length);
        return retValue;
    }

    @Override
    default byte[] fromByte(byte[] parameter) { return parameter; }

    @Override
    public default ToByte<byte[]> getInstance() { return new ToByteIdempotent() {}; }

}