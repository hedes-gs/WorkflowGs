package com.workflow.model;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteLong extends ToByte<Long> {

    @Override
    public default byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    @Override
    public default Long fromByte(byte[] parameter, int offset, int length) { return Bytes.toLong(parameter, offset); }

    @Override
    public default Long fromByte(byte[] parameter) { return Bytes.toLong(parameter); }

    @Override
    public default ToByte<Long> getInstance() { return new ToByteLong() {}; }

}
