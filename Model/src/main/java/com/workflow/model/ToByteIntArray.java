package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteIntArray extends ToByte<int[]> {
    @Override
    public default byte[] convert(int[] p) {
        byte[] retValue = new byte[2 + (Bytes.SIZEOF_INT * p.length)];
        Bytes.putAsShort(retValue, 0, (short) p.length);
        int index = Bytes.SIZEOF_SHORT;
        for (int element : p) {
            Bytes.putInt(retValue, index, element);
            index = index + Bytes.SIZEOF_INT;
        }
        return retValue;
    }

    @Override
    default int[] fromByte(byte[] parameter, int offset, int length) {
        int nbOfElements = Bytes.toShort(parameter, offset);
        int index = offset + Bytes.SIZEOF_SHORT;
        int[] retValue = new int[nbOfElements];
        for (int k = 0; k < nbOfElements; k++) {
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