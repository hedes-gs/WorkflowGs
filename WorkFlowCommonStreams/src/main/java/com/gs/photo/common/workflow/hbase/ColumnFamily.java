package com.gs.photo.common.workflow.hbase;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFamily {
    protected String              cfName;
    protected Map<byte[], byte[]> values;
    protected static Logger       LOGGER = LoggerFactory.getLogger(HbaseDataInformation.class);

    public void addColumn(String key, byte[] value) { this.values.put(key.getBytes(Charset.forName("UTF-8")), value); }

    public void addColumn(byte[] key, byte[] value) { this.values.put(key, value); }

    public ColumnFamily(String cfName) {
        this.cfName = cfName;
        this.values = new HashMap<>(5);
    }

    public Map<byte[], byte[]> getValues() { return this.values; }

    @Override
    public String toString() { return "ColumnFamily [cfName=" + this.cfName + ", values=" + this.values + "]"; }

}