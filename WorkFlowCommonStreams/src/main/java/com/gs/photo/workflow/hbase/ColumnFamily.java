package com.gs.photo.workflow.hbase;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class ColumnFamily {
    protected String              cfName;
    protected Map<byte[], byte[]> values;

    public void addColumn(String key, byte[] value) { this.values.put(key.getBytes(Charset.forName("UTF-8")), value); }

    public void addColumn(byte[] key, byte[] value) { this.values.put(key, value); }

    public ColumnFamily(String cfName) {
        this.cfName = cfName;
        this.values = new HashMap<>(5);
    }

    public Map<byte[], byte[]> getValues() { return this.values; }

}