package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

public abstract class GenericDAO<T extends HbaseData> extends AbstractDAO<T> implements IGenericDAO<T> {

	@Autowired
	protected Connection connection;

	protected TableName createTableIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
		Admin admin = connection.getAdmin();
		createNameSpaceIFNeeded(
			admin,
			hdi.getNameSpace());
		return createTableIfNeeded(
			admin,
			hdi.getTableName(),
			hdi.getFamilies());
	}

	protected void put(T[] hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				List<Put> puts = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							Put put = createHbasePut(
								getKey(
									hb,
									hbaseDataInformation),
								getCfList(
									hb,
									hbaseDataInformation));
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
	public void put(T[] hbaseData, Class<T> cl) {
		HbaseDataInformation<T> hbaseDataInformation = getHbaseDataInformation(
			cl);
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				List<Put> puts = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							Put put = createHbasePut(
								getKey(
									hb,
									hbaseDataInformation),
								getCfList(
									hb,
									hbaseDataInformation));
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
	public void delete(T[] hbaseData, Class<T> cl) {
		HbaseDataInformation<T> hbaseDataInformation = getHbaseDataInformation(
			cl);
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				List<Delete> dels = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							Delete delete = createHbaseDelete(
								getKey(
									hb,
									hbaseDataInformation));
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

	public void delete(T[] hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				List<Delete> dels = Arrays.stream(
					hbaseData).map(
						(hb) -> {
							Delete delete = createHbaseDelete(
								getKey(
									hb,
									hbaseDataInformation));
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

	public void delete(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				Delete delete = createHbaseDelete(
					getKey(
						hbaseData,
						hbaseDataInformation));
				table.delete(
					delete);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void put(T hbaseData, Class<T> cl) {
		HbaseDataInformation<T> hbaseDataInformation = getHbaseDataInformation(
			cl);
		try {
			try (Table table = getTable(
				connection,
				cl.getAnnotation(
					HbaseTableName.class).value())) {
				Put put = createHbasePut(
					getKey(
						hbaseData,
						hbaseDataInformation),
					getCfList(
						hbaseData,
						hbaseDataInformation));
				table.put(
					put);

			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public void put(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {

		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				Put put = createHbasePut(
					getKey(
						hbaseData,
						hbaseDataInformation),
					getCfList(
						hbaseData,
						hbaseDataInformation));
				table.put(
					put);

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

	protected Put createHbasePut(byte[] rowKey, Map<String, ColumnFamily> cfList) {
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

	protected Delete createHbaseDelete(byte[] rowKey) {
		Delete delete = new Delete(rowKey);
		return delete;
	}

	protected byte[] getKey(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {

		byte[] keyValue = new byte[hbaseDataInformation.getKeyLength()];
		hbaseDataInformation.buildKey(
			hbaseData,
			keyValue);
		return keyValue;
	}

	protected Map<String, ColumnFamily> getCfList(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
		return hbaseDataInformation.buildValue(
			hbaseData);
	}

	@Override
	public T get(T hbaseData, Class<T> cl) {
		HbaseDataInformation<T> hbaseDataInformation = getHbaseDataInformation(
			cl);
		Get get;
		byte[] key = getKey(
			hbaseData,
			hbaseDataInformation);
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
						hbaseDataInformation,
						result);
					return retValue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public T get(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {

		Get get;
		byte[] key = getKey(
			hbaseData,
			hbaseDataInformation);
		get = new Get(key);
		try {
			try (Table table = getTable(
				connection,
				hbaseDataInformation.getTable())) {
				Result result = table.get(
					get);
				if (result != null && !result.isEmpty()) {
					T retValue = toResult(
						hbaseDataInformation,
						result);
					return retValue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected T toResult(HbaseDataInformation<T> hbaseDataInformation, Result res) {
		try {
			T instance = hbaseDataInformation.getHbaseDataClass().newInstance();
			hbaseDataInformation.build(
				instance,
				res);

			return instance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void delete(T hbaseData, Class<T> cl) {
		@SuppressWarnings("unchecked")
		T[] a = (T[]) Array.newInstance(
			cl,
			1);
		a[0] = hbaseData;
		delete(
			a,
			cl);
	}

}
