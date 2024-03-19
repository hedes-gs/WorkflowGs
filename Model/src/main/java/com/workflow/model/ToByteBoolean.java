package com.workflow.model;

public interface ToByteBoolean extends ToByte<Boolean> {
    @Override

    public default byte[] convert(Boolean p) {
        byte[] retValue = new byte[1];
        retValue[0] = (byte) (p.booleanValue() ? -1 : 0);
        return retValue;
    }

    @Override
    public default Boolean fromByte(byte[] parameter, int offset, int length) { return parameter[offset] == -1; }

    @Override
    public default Boolean fromByte(byte[] parameter) { return parameter[0] == -1; }

    @Override
    public default ToByte<Boolean> getInstance() { return new ToByteBoolean() {}; }

}
