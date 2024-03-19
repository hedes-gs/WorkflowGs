package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteShortArray extends ToByte<short[]> {
    @Override
    public default byte[] convert(short[] p) {
        byte[] retValue = new byte[2 + (Bytes.SIZEOF_SHORT * p.length)];
        Bytes.putAsShort(retValue, 0, (short) p.length);
        int index = Bytes.SIZEOF_SHORT;
        for (short element : p) {
            Bytes.putShort(retValue, index, element);
            index = index + Bytes.SIZEOF_SHORT;
        }
        return retValue;
    }

    @Override
    default short[] fromByte(byte[] parameter, int offset, int length) {
        int nbOfElements = Bytes.toShort(parameter, offset);
        int index = offset + Bytes.SIZEOF_SHORT;
        short[] retValue = new short[nbOfElements];
        for (int k = 0; k < nbOfElements; k++) {
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