package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.gs.photo.workflow.hbase.HbaseDataFieldInformation;
import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.ToByte;

public abstract class AbstractDAO<T extends HbaseData> {

    @Autowired
    protected Connection              connection;

    @Value("${hbase.namespace}")
    protected String                  nameSpace;

    protected HbaseDataInformation<T> hbaseDataInformation;

    public HbaseDataInformation<T> getHbaseDataInformation(Class<T> cl) throws IOException {
        return this.hbaseDataInformation;
    }

    protected void createHbaseDataInformation(Class<T> cl) throws IOException {
        if (this.hbaseDataInformation == null) {
            this.hbaseDataInformation = new HbaseDataInformation<>(cl, this.nameSpace);
            AbstractDAO.buildHbaseDataInformation(cl, this.hbaseDataInformation);
            TableName tableName = this.createTableIfNeeded(this.hbaseDataInformation);
            if (tableName == null) {
                throw new IllegalArgumentException("Unable to get table name for " + this.hbaseDataInformation);
            }
            this.hbaseDataInformation.setTable(tableName);
        }
    }

    protected static <T extends HbaseData> void buildHbaseDataInformation(
        Class<T> cl,
        HbaseDataInformation<T> hbaseDataInformation
    ) {
        Arrays.asList(cl.getDeclaredFields())
            .forEach((field) -> {
                if (field.isAnnotationPresent(Column.class)) {
                    try {
                        Column cv = field.getAnnotation(Column.class);
                        Class<? extends ToByte<Object>> transformClass = (Class<? extends ToByte<Object>>) cv.toByte();
                        field.setAccessible(true);
                        HbaseDataFieldInformation value = new HbaseDataFieldInformation(field, transformClass, cv);
                        hbaseDataInformation.addField(value);
                    } catch (
                        IllegalArgumentException |
                        SecurityException e) {
                        e.printStackTrace();
                    }
                }
            });
        hbaseDataInformation.endOfInit();
    }

    protected TableName createTableIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
        Admin admin = this.connection.getAdmin();
        AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
        return AbstractDAO.createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies());
    }

    protected static TableName createTableIfNeeded(final Admin admin, String tableName, Collection<String> values)
        throws IOException {
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

    protected static void createNameSpaceIFNeeded(final Admin admin, String nameSpace) throws IOException {
        if (!AbstractDAO.namespaceExists(admin, nameSpace)) {
            admin.createNamespace(
                NamespaceDescriptor.create(nameSpace)
                    .build());
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
            System.arraycopy(b, 0, buffer, destPos, b.length);
            destPos = destPos + b.length;
        }
        return buffer;
    }

    protected static Table getTable(Connection connection, Admin admin, String value, Collection<String> columns)
        throws IOException {
        TableName hbaseTable = AbstractDAO.createTableIfNeeded(admin, value, columns);

        return connection.getTable(hbaseTable);
    }

    protected static Table getTable(Connection connection, String value) throws IOException {
        TableName hbaseTable = TableName.valueOf(value);

        return connection.getTable(hbaseTable);
    }

    protected static Table getTable(Connection connection, TableName hbaseTable) throws IOException {

        return connection.getTable(hbaseTable);
    }

    public AbstractDAO() { super(); }
}