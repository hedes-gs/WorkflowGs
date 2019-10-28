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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;
import com.workflow.model.ToByte;

public abstract class AbstractDAO<T extends HbaseData> {

	public static class HbaseDataFieldInformation implements Comparable<HbaseDataFieldInformation> {
		protected Field                           field;
		protected Class<? extends ToByte<Object>> transformClass;
		protected ToByte<Object>                  transformInstance;
		protected Column                          column;
		protected boolean                         partOfRowKey;
		protected int                             fixedWidth;
		protected int                             rowKeyNumber;
		protected String                          columnFamily;
		protected String                          hbaseName;
		public int                                offset;

		public byte[] toByte(Object valueToConvert) {
			byte[] convertedValue = this.transformInstance.convert(valueToConvert);
			return convertedValue;
		}

		public Object fromByte(byte[] buffer, int offset, int length) {
			Object value = this.transformInstance.fromByte(buffer,
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
			this.transformInstance = (ToByte<Object>) Proxy.newProxyInstance(
					Thread.currentThread().getContextClassLoader(),
					new Class[] { transformClass },
					(proxy, method, args) -> {
						Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class);
						constructor.setAccessible(true);
						return constructor.newInstance(transformClass).in(transformClass).unreflectSpecial(method,
								transformClass).bindTo(proxy).invokeWithArguments(args);

					});
		}

		@Override
		public int compareTo(HbaseDataFieldInformation o) {
			return Integer.compare(this.rowKeyNumber,
					o.rowKeyNumber);
		}

	}

	public static class HbaseDataInformation<T extends HbaseData> {
		private final Set<HbaseDataFieldInformation> keyFieldsData;
		private final Set<String>                    columnFamily;
		private final Set<HbaseDataFieldInformation> fieldsData;
		private int                                  keyLength = 0;
		private final String                         tableName;
		private TableName                            table;
		private Class<T>                             hbaseDataClass;
		private final String                         nameSpace;

		public int getKeyLength() {
			return this.keyLength;
		}

		public void setKeyLength(int keyLength) {
			this.keyLength = keyLength;
		}

		public String getTableName() {
			return this.tableName;
		}

		public void addField(HbaseDataFieldInformation hdfi) {

			if (hdfi.partOfRowKey) {
				this.keyLength = this.keyLength + hdfi.fixedWidth;
				this.keyFieldsData.add(hdfi);
			} else {
				String cf = hdfi.columnFamily;
				this.columnFamily.add(cf);
				this.fieldsData.add(hdfi);
			}
		}

		public HbaseDataInformation(
				Class<T> cl,
				String prefix) {
			this.fieldsData = new TreeSet<>();
			this.keyFieldsData = new TreeSet<>();
			this.columnFamily = new TreeSet<>();
			this.tableName = prefix + ":" + cl.getAnnotation(HbaseTableName.class).value();
			this.hbaseDataClass = cl;
			this.nameSpace = prefix;
		}

		public void endOfInit() {
			int offset = 0;
			for (HbaseDataFieldInformation v : this.keyFieldsData) {
				v.offset = offset;
				offset = offset + v.fixedWidth;
			}
		}

		public Map<String, ColumnFamily> buildValue(T hbaseData) {
			Map<String, ColumnFamily> cfList = new HashMap<>();

			this.fieldsData.forEach((hdfi) -> {
				try {
					hdfi.field.setAccessible(true);
					Object valueToConvert = hdfi.field.get(hbaseData);
					byte[] convertedValue = hdfi.toByte(valueToConvert);
					String cf = hdfi.columnFamily;
					ColumnFamily value = cfList.get(cf);
					if (value == null) {
						value = new ColumnFamily(cf);
						cfList.put(cf,
								value);
					}
					value.addColumn(hdfi.hbaseName,
							convertedValue);
				} catch (SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			return cfList;

		}

		public void buildKey(T hbaseData, byte[] keyValue) {
			Arrays.fill(keyValue,
					(byte) 0x20);
			this.keyFieldsData.forEach((hdfi) -> {
				try {
					hdfi.field.setAccessible(true);
					Object valueToConvert = hdfi.field.get(hbaseData);
					byte[] convertedValue = hdfi.toByte(valueToConvert);
					System.arraycopy(convertedValue,
							0,
							keyValue,
							hdfi.offset,
							convertedValue.length);
				} catch (SecurityException e) {

					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}

		public Collection<String> getFamilies() {
			return this.columnFamily;
		}

		public void build(T instance, Result res) {
			byte[] row = res.getRow();
			this.keyFieldsData.forEach((hdfi) -> {
				Object v = null;
				if (hdfi.partOfRowKey) {
					v = hdfi.fromByte(row,
							hdfi.offset,
							hdfi.fixedWidth);
				}
				try {
					hdfi.field.set(instance,
							v);
				} catch (
						IllegalArgumentException |
						IllegalAccessException e) {
					e.printStackTrace();
				}
			});
			this.fieldsData.forEach((hdfi) -> {
				Object v = null;
				byte[] value = res.getValue(hdfi.columnFamily.getBytes(),
						hdfi.hbaseName.getBytes());
				v = hdfi.fromByte(value,
						0,
						value.length);
				try {
					hdfi.field.set(instance,
							v);
				} catch (
						IllegalArgumentException |
						IllegalAccessException e) {
					e.printStackTrace();
				}
			});
		}

		public TableName getTable() {
			return this.table;
		}

		public void setTable(TableName table) {
			this.table = table;
		}

		public Class<T> getHbaseDataClass() {
			// TODO Auto-generated method stub
			return this.hbaseDataClass;
		}

		public String getNameSpace() {
			return this.nameSpace;
		}

	}

	public static final ThreadLocal<Map<Class<? extends HbaseData>, HbaseDataInformation>> HBASE_DATA_THREAD_LOCAL = new ThreadLocal<Map<Class<? extends HbaseData>, HbaseDataInformation>>() {

		@Override
		protected Map<Class<? extends HbaseData>, HbaseDataInformation> initialValue() {
			return new HashMap<>();
		}
	};

	public static <T extends HbaseData> HbaseDataInformation<T> getHbaseDataInformation(Class<T> cl) {
		HbaseDataInformation<T> hbaseDataInformation = AbstractDAO.HBASE_DATA_THREAD_LOCAL.get().get(cl);
		if (hbaseDataInformation == null) {
			hbaseDataInformation = new HbaseDataInformation<>(cl, null);
			AbstractDAO.buildHbaseDataInformation(cl,
					hbaseDataInformation);
			AbstractDAO.HBASE_DATA_THREAD_LOCAL.get().put(cl,
					hbaseDataInformation);
		}
		return hbaseDataInformation;
	}

	protected static <T extends HbaseData> void buildHbaseDataInformation(Class<T> cl,
			HbaseDataInformation<T> hbaseDataInformation) {
		Arrays.asList(cl.getDeclaredFields()).forEach((field) -> {
			if (field.isAnnotationPresent(Column.class)) {
				try {
					Column cv = field.getAnnotation(Column.class);
					Class<? extends ToByte<Object>> transformClass = (Class<? extends ToByte<Object>>) cv.toByte();
					field.setAccessible(true);

					@SuppressWarnings("unchecked")
					ToByte<Object> toByteInterface = (ToByte<Object>) Proxy.newProxyInstance(
							Thread.currentThread().getContextClassLoader(),
							new Class[] { transformClass },
							(proxy, method, args) -> {

								Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class);
								constructor.setAccessible(true);
								return constructor.newInstance(transformClass).in(transformClass)
										.unreflectSpecial(method,
												transformClass)
										.bindTo(proxy).invokeWithArguments(args);

							});
					HbaseDataFieldInformation value = new HbaseDataFieldInformation(field, transformClass, cv);
					hbaseDataInformation.addField(value);
				} catch (
						IllegalArgumentException |
						SecurityException e) {
					e.printStackTrace();
				}
			}
		});
		int offset = 0;
		hbaseDataInformation.endOfInit();

	}

	protected static final String MY_NAMESPACE_NAME = "myTestNamespace";

	public static class ColumnFamily {
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

	}

	protected static TableName createTableIfNeeded(final Admin admin, String tableName, Collection<String> values)
			throws IOException {
		AbstractDAO.createNameSpaceIFNeeded(admin);
		TableName hbaseTable = TableName.valueOf(tableName);
		if (!admin.tableExists(hbaseTable)) {

			TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hbaseTable);

			values.forEach((cfName) -> {
				builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cfName));

			});
			admin.createTable(builder.build());
		}
		return hbaseTable;
	}

	protected static void createNameSpaceIFNeeded(final Admin admin) throws IOException {
		if (!AbstractDAO.namespaceExists(admin,
				AbstractDAO.MY_NAMESPACE_NAME)) {
			admin.createNamespace(NamespaceDescriptor.create(AbstractDAO.MY_NAMESPACE_NAME).build());
		}
	}

	protected static void createNameSpaceIFNeeded(final Admin admin, String nameSpace) throws IOException {
		if (!AbstractDAO.namespaceExists(admin,
				nameSpace)) {
			admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
		}
	}

	protected static boolean namespaceExists(final Admin admin, final String namespaceName) throws IOException {
		try {
			admin.getNamespaceDescriptor(namespaceName);
		} catch (NamespaceNotFoundException e) {
			return false;
		}
		return true;
	}

	protected static byte[] buildKey(List<byte[]> keysElements, int length) {
		byte[] buffer = new byte[length];
		int destPos = 0;
		for (byte[] b : keysElements) {
			System.arraycopy(b,
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
		TableName hbaseTable = AbstractDAO.createTableIfNeeded(admin,
				value,
				columns);

		return connection.getTable(hbaseTable);
	}

	protected static Table getTable(Connection connection, String value) throws IOException {
		TableName hbaseTable = TableName.valueOf(value);

		return connection.getTable(hbaseTable);
	}

	protected static Table getTable(Connection connection, TableName hbaseTable) throws IOException {

		return connection.getTable(hbaseTable);
	}

	public AbstractDAO() {
		super();
	}
}