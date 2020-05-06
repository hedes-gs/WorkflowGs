package com.gs.photo.workflow.hbase;

import java.util.HashMap;
import java.util.Map;

public class ColumnFamily {
	protected String              cfName;
	protected Map<String, byte[]> values;

	public void addColumn(String key, byte[] value) {
		this.values.put(key,
			value);
	}

	public ColumnFamily(
		String cfName) {
		this.cfName = cfName;
		this.values = new HashMap<>(5);
	}

	public Map<String, byte[]> getValues() {
		return this.values;
	}

}