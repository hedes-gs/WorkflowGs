package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteInt extends ToByte<Integer> {

    @Override
    public default byte[] convert(Integer p) {
        byte[] retValue = new byte[4];
        Bytes.putInt(retValue, 0, p);
        return retValue;
    }

    @Override
    public default Integer fromByte(byte[] parameter, int offset, int length) { return Bytes.toInt(parameter, offset); }

    @Override
    public default Integer fromByte(byte[] parameter) { return Bytes.toInt(parameter); }

    @Override
    public default ToByte<Integer> getInstance() { return new ToByteInt() {}; }

}
