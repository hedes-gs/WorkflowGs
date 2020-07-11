package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteShortArray extends ToByte<short[]> {
    @Override
    public default byte[] convert(short[] p) {
        byte[] retValue = new byte[1 + (2 * p.length)];
        Bytes.putByte(retValue, 0, (byte) p.length);
        int index = 1;
        for (short element : p) {
            Bytes.putShort(retValue, index, element);
            index = index + Bytes.SIZEOF_SHORT;
        }
        return retValue;
    }

    @Override
    default short[] fromByte(byte[] parameter, int offset, int length) {
        int nbOfElements = parameter[offset];
        int index = offset + 1;
        short[] retValue = new short[nbOfElements];
        for (int k = 0; k < retValue.length; k++) {
            retValue[k] = Bytes.toShort(parameter, index);
            index = index + Bytes.SIZEOF_SHORT;
        }
        return retValue;
    }

    @Override
    default short[] fromByte(byte[] parameter) {
        int nbOfElements = parameter[0];
        int index = 1;
        short[] retValue = new short[nbOfElements];
        for (int k = 0; k < retValue.length; k++) {
            retValue[k] = Bytes.toShort(parameter, index);
            index = index + Bytes.SIZEOF_SHORT;
        }
        return retValue;
    }

    @Override
    public default ToByte<short[]> getInstance() { return new ToByteShortArray() {}; }

}