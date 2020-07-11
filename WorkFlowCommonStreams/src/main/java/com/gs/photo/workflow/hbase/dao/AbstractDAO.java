package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
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

    // Common name when stats are needed (nb of elements for a collection for
    // instance)

    protected static final String     COLUMN_STAT_NAME           = "stats";
    protected static final String     FAMILY_IMGS_NAME           = "imgs";
    protected static final String     FAMILY_STATS_NAME          = "fstats";
    protected static final byte[]     COLUMN_STAT_AS_BYTES       = AbstractDAO.COLUMN_STAT_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     FAMILY_IMGS_NAME_AS_BYTES  = AbstractDAO.FAMILY_IMGS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     FAMILY_STATS_NAME_AS_BYTES = AbstractDAO.FAMILY_STATS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     TRUE_VALUE                 = new byte[] { 1 };

    @Autowired
    protected Connection              connection;

    @Value("${hbase.namespace}")
    protected String                  nameSpace;

    protected HbaseDataInformation<T> hbaseDataInformation;

    protected BufferedMutator         bufferedMutator;

    public HbaseDataInformation<T> getHbaseDataInformation() throws IOException {
        return this.hbaseDataInformation;
    }

    public void flush() throws IOException { this.bufferedMutator.flush(); }

    protected void createHbaseDataInformation(Class<T> cl) throws IOException {
        if (this.hbaseDataInformation == null) {
            this.hbaseDataInformation = new HbaseDataInformation<>(cl, this.nameSpace);
            AbstractDAO.buildHbaseDataInformation(cl, this.hbaseDataInformation);
            TableName tableName = this.createTableIfNeeded(this.hbaseDataInformation);
            if (tableName == null) {
                throw new IllegalArgumentException("Unable to get table name for " + this.hbaseDataInformation);
            }
            this.hbaseDataInformation.setTable(tableName);
            this.bufferedMutator = AbstractDAO.getBufferedMutator(this.connection, tableName);
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

    protected static BufferedMutator getBufferedMutator(Connection connection, TableName hbaseTable)
        throws IOException {
        return connection.getBufferedMutator(new BufferedMutatorParams(hbaseTable).writeBufferSize(5 * 1024 * 1024));
    }

    protected static byte[] toBytes(String imageId) { return imageId.getBytes(Charset.forName("UTF-8")); }

    public AbstractDAO() { super(); }
}