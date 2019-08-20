package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

@Component
public class GenericDAO extends AbstractDAO implements IGenericDAO {

	@Autowired
	protected Configuration hbaseConfiguration;

	@Override
	public <T extends HbaseData> void put(T[] hbaseData, Class<T> cl) {
		List<byte[]> keysElements = new ArrayList<byte[]>();
		Map<String, ColumnFamily> cfList = new HashMap<>();
		int[] length = { 0 };
		Collection<String> familiesList = extractColumnFamilies(
			cl);

		try {
			try (Connection connection = ConnectionFactory.createConnection(
				hbaseConfiguration); Admin admin = connection.getAdmin()) {

				try (Table table = getTable(
					connection,
					admin,
					cl.getAnnotation(
						HbaseTableName.class).value(),
					familiesList)) {
					Arrays.stream(
						hbaseData).forEach(
							(hb) -> {
								extractHbaseMetadataFromClass(
									hb,
									cl,
									keysElements,
									cfList,
									length);
								recordInHbase(
									keysElements,
									cfList,
									length,
									table);
							});
				}
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
		extractHbaseMetadataFromClass(
			hbaseData,
			cl,
			keysElements,
			cfList,
			length);

		try (Connection connection = ConnectionFactory.createConnection(
			hbaseConfiguration); Admin admin = connection.getAdmin()) {

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

	public void recordInHbase(List<byte[]> keysElements, Map<String, ColumnFamily> cfList, int[] length, Table table) {
		byte[] rowKey = getKey(
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

		try {
			table.put(
				put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public <T extends HbaseData> void extractHbaseMetadataFromClass(T hbaseData, Class<T> cl, List<byte[]> keysElements,
			Map<String, ColumnFamily> cfList, int[] length) {
		HbaseDataInformation hbaseDataInformation = getHbaseDataInformation(
			cl);
		hbaseDataInformation.getFieldsData().forEach(
			(hdfi) -> {

			});
	}

	public <T extends HbaseData> void retrieve(T hbaseData, Class<T> cl, List<byte[]> keysElements,
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

}
