package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteIntArray extends ToByte<int[]> {
    @Override
    public default byte[] convert(int[] p) {
        byte[] retValue = new byte[1 + (Bytes.SIZEOF_INT * p.length)];
        Bytes.putByte(retValue, 0, (byte) p.length);
        int index = 1;
        for (int element : p) {
            Bytes.putInt(retValue, index, element);
            index = index + Bytes.SIZEOF_INT;
        }
        return retValue;
    }

    @Override
    default int[] fromByte(byte[] parameter, int offset, int length) {
        int nbOfElements = parameter[offset];
        int index = offset + 1;
        int[] retValue = new int[nbOfElements];
        for (int k = 0; k < retValue.length; k++) {
            retValue[k] = Bytes.toInt(parameter, index);
            index = index + Bytes.SIZEOF_INT;
        }
        return retValue;
    }

    @Override
    default int[] fromByte(byte[] parameter) {
        int nbOfElements = parameter[0];
        int index = 1;
        int[] retValue = new int[nbOfElements];
        for (int k = 0; k < retValue.length; k++) {
            retValue[k] = Bytes.toInt(parameter, index);
            index = index + Bytes.SIZEOF_INT;
        }
        return retValue;
    }

    @Override
    public default ToByte<int[]> getInstance() { return new ToByteIntArray() {}; }
}