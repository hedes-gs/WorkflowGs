package com.gs.photos.serializers;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.serialization.Serializer;

public class IntArraySerializer implements Serializer<int[]> {
    @Override
    public byte[] serialize(String topic, int[] data) {
        byte[] retValue = new byte[Bytes.SIZEOF_INT * data.length];
        int offset = 0;
        for (int d : data) {
            offset = Bytes.putInt(retValue, offset, d);
        }
        return retValue;

    }
}