package com.gs.photos.serializers;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.serialization.Deserializer;

public class IntArrayDeserializer implements Deserializer<int[]> {

    @Override
    public int[] deserialize(String topic, byte[] data) {
        int[] retValue = new int[data.length / Bytes.SIZEOF_INT];
        int offset = 0;
        for (int k = 0; k < retValue.length; k++) {
            retValue[k] = Bytes.readAsInt(data, offset, Bytes.SIZEOF_INT);
            offset = offset + Bytes.SIZEOF_INT;
        }
        return retValue;
    }
}
