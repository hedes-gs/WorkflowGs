package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.ToByte;

public abstract class AbstractDAO {

	public static class HbaseDataFieldInformation implements Comparable<HbaseDataFieldInformation> {
		protected Field field;
		protected Class<? extends ToByte<Object>> transformClass;
		protected ToByte<Object> transformInstance;
		protected Column column;
		protected boolean partOfRowKey;
		protected int fixedWidth;
		protected int rowKeyNumber;
		protected String columnFamily;
		protected String hbaseName;
		public int offset;

		public byte[] toByte(Object valueToConvert) {
			byte[] convertedValue = transformInstance.convert(
				valueToConvert);
			return convertedValue;
		}

		public Object fromByte(byte[] buffer, int offset, int length) {
			Object value = transformInstance.fromByte(
				buffer,
				offset,
				length);
			return value;
		}

		@SuppressWarnings("unchecked")
		public HbaseDataFieldInformation(
				Field field,
				Class<? extends ToByte<Object>> transformClass,
				Column column) {
			super();
			this.field = field;
			this.transformClass = transformClass;
			this.column = column;
			this.partOfRowKey = this.column.isPartOfRowkey();
			this.fixedWidth = this.column.fixedWidth();
			this.rowKeyNumber = column.rowKeyNumber();
			this.columnFamily = column.columnFamily();
			this.hbaseName = column.hbaseName();
			transformInstance = (ToByte<Object>) Proxy.newProxyInstance(
				Thread.currentThread().getContextClassLoader(),
				new Class[] { transformClass },
				(proxy, method, args) -> {
					Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(
						Class.class);
					constructor.setAccessible(
						true);
					return constructor.newInstance(
						transformClass).in(
							transformClass).unreflectSpecial(
								method,
								transformClass)
							.bindTo(
								proxy)
							.invokeWithArguments(
								args);

				});
		}

		@Override
		public int compareTo(HbaseDataFieldInformation o) {
			return Integer.compare(
				rowKeyNumber,
				o.rowKeyNumber);
		}

	}

	public static class HbaseDataInformation {
		private final Set<HbaseDataFieldInformation> fieldsData;

		public Set<HbaseDataFieldInformation> getFieldsData() {
			return fieldsData;
		}

		public HbaseDataInformation() {
			fieldsData = new TreeSet<>();
		}
	}

	public static final ThreadLocal<Map<Class<? extends HbaseData>, HbaseDataInformation>> HBASE_DATA_THREAD_LOCAL = new ThreadLocal<Map<Class<? extends HbaseData>, HbaseDataInformation>>() {

		@Override
		protected Map<Class<? extends HbaseData>, HbaseDataInformation> initialValue() {
			return new HashMap<>();
		}
	};

	public static <T extends HbaseData> HbaseDataInformation getHbaseDataInformation(Class<T> cl) {
		HbaseDataInformation hbaseDataInformation = HBASE_DATA_THREAD_LOCAL.get().get(
			cl);
		if (hbaseDataInformation == null) {
			hbaseDataInformation = new HbaseDataInformation();
			buildHbaseDataInformation(
				cl,
				hbaseDataInformation);
			HBASE_DATA_THREAD_LOCAL.get().put(
				cl,
				hbaseDataInformation);
		}
		return hbaseDataInformation;
	}

	private static <T extends HbaseData> void buildHbaseDataInformation(Class<T> cl,
			HbaseDataInformation hbaseDataInformation) {
		Arrays.asList(
			cl.getDeclaredFields()).forEach(
				(field) -> {
					if (field.isAnnotationPresent(
						Column.class)) {
						try {
							Column cv = field.getAnnotation(
								Column.class);
							Class<? extends ToByte<Object>> transformClass = (Class<? extends ToByte<Object>>) cv
									.toByte();
							field.setAccessible(
								true);

							@SuppressWarnings("unchecked")
							ToByte<Object> toByteInterface = (ToByte<Object>) Proxy.newProxyInstance(
								Thread.currentThread().getContextClassLoader(),
								new Class[] { transformClass },
								(proxy, method, args) -> {

									Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(
										Class.class);
									constructor.setAccessible(
										true);
									return constructor.newInstance(
										transformClass).in(
											transformClass).unreflectSpecial(
												method,
												transformClass)
											.bindTo(
												proxy)
											.invokeWithArguments(
												args);

								});
							HbaseDataFieldInformation value = new HbaseDataFieldInformation(field, transformClass, cv);
							hbaseDataInformation.getFieldsData().add(
								value);
						} catch (IllegalArgumentException | SecurityException e) {
							e.printStackTrace();
						}
					}
				});
		int offset = 0;
		for (HbaseDataFieldInformation v : hbaseDataInformation.getFieldsData()) {
			v.offset = offset;
			offset = offset + v.fixedWidth;
		}
	}

	protected static final String MY_NAMESPACE_NAME = "myTestNamespace";

	public static class ColumnFamily {
		protected String cfName;
		protected Map<String, byte[]> values;

		public void addColumn(String key, byte[] value) {
			values.put(
				key,
				value);
		}

		public ColumnFamily(
				String cfName) {
			this.cfName = cfName;
			values = new HashMap<>(5);
		}

	}

	protected static TableName createTableIfNeeded(final Admin admin, String tableName, Collection<String> values)
			throws IOException {
		if (!namespaceExists(
			admin,
			MY_NAMESPACE_NAME)) {
			System.out.println(
				"Creating Namespace [" + MY_NAMESPACE_NAME + "].");
			admin.createNamespace(
				NamespaceDescriptor.create(
					MY_NAMESPACE_NAME).build());
		}
		TableName hbaseTable = TableName.valueOf(
			tableName);
		if (!admin.tableExists(
			hbaseTable)) {

			TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(
				hbaseTable);

			values.forEach(
				(cfName) -> {
					builder.setColumnFamily(
						ColumnFamilyDescriptorBuilder.of(
							cfName));

				});
			admin.createTable(
				builder.build());
		}
		return hbaseTable;
	}

	protected static boolean namespaceExists(final Admin admin, final String namespaceName) throws IOException {
		try {
			admin.getNamespaceDescriptor(
				namespaceName);
		} catch (NamespaceNotFoundException e) {
			return false;
		}
		return true;
	}

	protected static byte[] buildKey(List<byte[]> keysElements, int length) {
		byte[] buffer = new byte[length];
		int destPos = 0;
		for (byte[] b : keysElements) {
			System.arraycopy(
				b,
				0,
				buffer,
				destPos,
				b.length);
			destPos = destPos + b.length;
		}
		return buffer;
	}

	protected static Table getTable(Connection connection, Admin admin, String value, Collection<String> columns)
			throws IOException {
		TableName hbaseTable = createTableIfNeeded(
			admin,
			value,
			columns);

		return connection.getTable(
			hbaseTable);
	}

	protected static Table getTable(Connection connection, String tableName) throws IOException {
		TableName hbaseTable = TableName.valueOf(
			tableName);
		return connection.getTable(
			hbaseTable);
	}

	public AbstractDAO() {
		super();
	}

	public abstract <T extends HbaseData> void put(T hbaseData, Class<T> cl);

	public abstract <T extends HbaseData> void put(T[] hbaseData, Class<T> cl);

	public abstract <T extends HbaseData> T get(T hbaseData, Class<T> cl);

	public abstract <T extends HbaseData> void delete(T hbaseData, Class<T> cl);

	public abstract <T extends HbaseData> void delete(T[] hbaseData, Class<T> cl);

}