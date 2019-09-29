package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

@Component
public class GenericDAO extends AbstractDAO implements IGenericDAO {

	@Autowired
	protected Configuration hbaseConfiguration;

	protected Connection connection;

	@PostConstruct
	protected void init() throws IOException {
		this.connection = ConnectionFactory.createConnection(
			hbaseConfiguration);
	}

	@Override
	public <T extends HbaseData> void put(T[] hbaseData, Class<T> cl) {
		List<byte[]> keysElements = new ArrayList<byte[]>();
		Map<String, ColumnFamily> cfList = new HashMap<>();
		int[] length = { 0 };
		Collection<String> familiesList = extractColumnFamilies(
			cl);

		try {
			Admin admin = connection.getAdmin();
			try (Table table = getTable(
				connection,
				admin,
				cl.getAnnotation(
					HbaseTableName.class).value(),
				familiesList)) {
				List<Put> puts = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							keysElements.clear();
							cfList.clear();
							length[0] = 0;
							retrieve(
								hb,
								cl,
								keysElements,
								cfList,
								length);
							Put put = createHbasePut(
								keysElements,
								cfList,
								length);
							return put;
						}).collect(
							Collectors.toList());
				table.put(
					puts);
				puts.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public <T extends HbaseData> void delete(T[] hbaseData, Class<T> cl) {
		List<byte[]> keysElements = new ArrayList<byte[]>();
		Map<String, ColumnFamily> cfList = new HashMap<>();
		int[] length = { 0 };
		Collection<String> familiesList = extractColumnFamilies(
			cl);

		try {
			Admin admin = connection.getAdmin();
			try (Table table = getTable(
				connection,
				admin,
				cl.getAnnotation(
					HbaseTableName.class).value(),
				familiesList)) {
				List<Delete> dels = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							keysElements.clear();
							cfList.clear();
							length[0] = 0;
							retrieve(
								hb,
								cl,
								keysElements,
								cfList,
								length);
							Delete delete = createHbaseDelete(
								keysElements,
								cfList,
								length);
							return delete;
						}).collect(
							Collectors.toList());
				table.delete(
					dels);
				dels.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private <T extends HbaseData> Set<String> extractColumnFamilies(Class<T> cl) {
		Set<String> retValue = new TreeSet<>();
		HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
			cl);
		hbaseDataInformation.getFieldsData().forEach(
			(hdfi) -> {
				try {
					hdfi.field.setAccessible(
						true);
					if (!hdfi.partOfRowKey) {
						String cf = hdfi.columnFamily;
						retValue.add(
							cf);
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			});
		return retValue;
	}

	@Override
	public <T extends HbaseData> void put(T hbaseData, Class<T> cl) {

		List<byte[]> keysElements = new ArrayList<byte[]>();
		Map<String, ColumnFamily> cfList = new HashMap<>();
		int[] length = { 0 };
		Collection<String> familiesList = extractColumnFamilies(
			cl);
		retrieve(
			hbaseData,
			cl,
			keysElements,
			cfList,
			length);

		try {
			Admin admin = connection.getAdmin();

			try (Table table = getTable(
				connection,
				admin,
				cl.getAnnotation(
					HbaseTableName.class).value(),
				familiesList)) {
				recordInHbase(
					keysElements,
					cfList,
					length,
					table);

			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public static String toHex(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		sb.append(
			"[ ");
		for (byte b : bytes) {
			sb.append(
				String.format(
					"0x%02X ",
					b));
		}
		sb.append(
			"]");
		return sb.toString();
	}

	public void recordInHbase(List<byte[]> keysElements, Map<String, ColumnFamily> cfList, int[] length, Table table) {
		Put put = createHbasePut(
			keysElements,
			cfList,
			length);

		try {
			table.put(
				put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected Put createHbasePut(List<byte[]> keysElements, Map<String, ColumnFamily> cfList, int[] length) {
		byte[] rowKey = buildKey(
			keysElements,
			length[0]);
		Put put = new Put(rowKey);
		cfList.entrySet().forEach(
			(cf) -> {
				byte[] bytesOfCfName = cf.getKey().getBytes();
				cf.getValue().values.entrySet().forEach(
					(q) -> {
						put.addColumn(
							bytesOfCfName,
							q.getKey().getBytes(),
							q.getValue());
					});
			});
		return put;
	}

	protected Delete createHbaseDelete(List<byte[]> keysElements, Map<String, ColumnFamily> cfList, int[] length) {
		byte[] rowKey = buildKey(
			keysElements,
			length[0]);
		Delete delete = new Delete(rowKey);
		return delete;
	}

	protected <T extends HbaseData> void getKeys(T hbaseData, Class<T> cl, List<byte[]> keysElements, int[] length) {
		HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
			cl);
		hbaseDataInformation.getFieldsData().forEach(
			(hdfi) -> {
				try {

					if (hdfi.partOfRowKey) {
						hdfi.field.setAccessible(
							true);
						Object valueToConvert = hdfi.field.get(
							hbaseData);
						byte[] convertedValue = hdfi.toByte(
							valueToConvert);
						byte[] keyPadded = new byte[hdfi.fixedWidth];
						Arrays.fill(
							keyPadded,
							(byte) 0x20);
						System.arraycopy(
							convertedValue,
							0,
							keyPadded,
							0,
							convertedValue.length);
						keysElements.add(
							hdfi.rowKeyNumber,
							keyPadded);
						length[0] = length[0] + hdfi.fixedWidth;
					}
				} catch (IllegalArgumentException | IllegalAccessException e) {
					e.printStackTrace();
				}
			});
	}

	protected <T extends HbaseData> void retrieve(T hbaseData, Class<T> cl, List<byte[]> keysElements,
			Map<String, ColumnFamily> cfList, int[] length) {
		HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
			cl);
		hbaseDataInformation.getFieldsData().forEach(
			(hdfi) -> {
				try {
					hdfi.field.setAccessible(
						true);
					Object valueToConvert = hdfi.field.get(
						hbaseData);
					byte[] convertedValue = hdfi.toByte(
						valueToConvert);
					if (hdfi.partOfRowKey) {
						byte[] keyPadded = new byte[hdfi.fixedWidth];
						Arrays.fill(
							keyPadded,
							(byte) 0x20);
						System.arraycopy(
							convertedValue,
							0,
							keyPadded,
							0,
							convertedValue.length);
						keysElements.add(
							hdfi.rowKeyNumber,
							keyPadded);
						length[0] = length[0] + hdfi.fixedWidth;
					} else {
						String cf = hdfi.columnFamily;
						ColumnFamily value = cfList.get(
							cf);
						if (value == null) {
							value = new ColumnFamily(cf);
							cfList.put(
								cf,
								value);
						}
						value.addColumn(
							hdfi.hbaseName,
							convertedValue);
					}
				} catch (IllegalArgumentException | IllegalAccessException e) {
					e.printStackTrace();
				}
			});
	}

	@Override
	public <T extends HbaseData> T get(T hbaseData, Class<T> cl) {
		Get get;
		List<byte[]> keysElements = new ArrayList<byte[]>();
		int[] length = { 0 };
		getKeys(
			hbaseData,
			cl,
			keysElements,
			length);
		byte[] key = buildKey(
			keysElements,
			length[0]);
		get = new Get(key);
		try {
			try (Table table = getTable(
				connection,
				cl.getAnnotation(
					HbaseTableName.class).value())) {
				Result result = table.get(
					get);
				if (result != null && !result.isEmpty()) {
					T retValue = toResult(
						cl,
						result);
					return retValue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected <T extends HbaseData> T toResult(Class<T> cl, Result res) {
		try {
			T instance = cl.newInstance();
			byte[] row = res.getRow();
			HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
				cl);
			hbaseDataInformation.getFieldsData().forEach(
				(hdfi) -> {
					Object v = null;
					if (hdfi.partOfRowKey) {
						v = hdfi.fromByte(
							row,
							hdfi.offset,
							hdfi.fixedWidth);
					} else {
						byte[] value = res.getValue(
							hdfi.columnFamily.getBytes(),
							hdfi.hbaseName.getBytes());
						v = hdfi.fromByte(
							value,
							0,
							value.length);
					}
					try {
						hdfi.field.set(
							instance,
							v);
					} catch (IllegalArgumentException | IllegalAccessException e) {
						e.printStackTrace();
					}
				});
			return instance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public <T extends HbaseData> void delete(T hbaseData, Class<T> cl) {
		@SuppressWarnings("unchecked")
		T[] a = (T[]) Array.newInstance(
			cl,
			1);

		delete(
			a,
			cl);
	}

}
