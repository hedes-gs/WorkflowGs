package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import com.gs.photo.workflow.hbase.HbaseDataFieldInformation;
import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.ToByte;

public abstract class AbstractDAO<T extends HbaseData> {

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
			AbstractDAO.HBASE_DATA_THREAD_LOCAL.get()
				.put(cl,
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
							return constructor.newInstance(transformClass)
								.in(transformClass)
								.unreflectSpecial(method,
									transformClass)
								.bindTo(proxy)
								.invokeWithArguments(args);

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