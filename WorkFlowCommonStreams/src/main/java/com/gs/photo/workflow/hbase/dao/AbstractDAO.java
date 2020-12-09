package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseData;

public abstract class AbstractDAO<T extends HbaseData> {

    protected static Logger           LOGGER                               = LoggerFactory.getLogger(AbstractDAO.class);

    // Common name when stats are needed (nb of elements for a collection for
    // instance)

    public static final byte[]        TABLE_PAGE_DESC_COLUMN_FAMILY        = "max_min".getBytes();
    public static final byte[]        TABLE_PAGE_LIST_COLUMN_FAMILY        = "list".getBytes();
    public static final byte[]        TABLE_PAGE_INFOS_COLUMN_FAMILY       = "infos".getBytes();
    public static final byte[]        TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS  = "nbOfElements".getBytes();
    public static final byte[]        TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER  = "max".getBytes();
    public static final byte[]        TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER  = "min".getBytes();
    protected static final String     FAMILY_INFOS_COL_NB_OF_ELEM          = "nbOfElements";
    protected static final String     FAMILY_IMGS_NAME                     = "imgs";
    protected static final String     FAMILY_INFOS_NAME                    = "infos";

    protected static final byte[]     FAMILY_INFOS_COL_NB_OF_ELEM_AS_BYTES = AbstractDAO.FAMILY_INFOS_COL_NB_OF_ELEM
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     FAMILY_IMGS_NAME_AS_BYTES            = AbstractDAO.FAMILY_IMGS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     FAMILY_INFOS_NAME_AS_BYTES           = AbstractDAO.FAMILY_INFOS_NAME
        .getBytes(Charset.forName("UTF-8"));
    protected static final byte[]     TRUE_VALUE                           = new byte[] { 1 };

    @Autowired
    protected Connection              connection;

    @Value("${hbase.namespace}")
    protected String                  nameSpace;

    protected HbaseDataInformation<T> hbaseDataInformation;

    protected BufferedMutator         bufferedMutator;

    public HbaseDataInformation<T> getHbaseDataInformation() { return this.hbaseDataInformation; }

    public void flush() throws IOException {
        try {

            this.bufferedMutator.flush();
        } catch (RetriesExhaustedWithDetailsException e) {
            AbstractDAO.LOGGER.error(" Retries error ! {}", ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (IOException e) {
            throw e;
        }

    }

    protected void createHbaseDataInformation(Class<T> cl) throws IOException {
        if (this.hbaseDataInformation == null) {
            this.hbaseDataInformation = new HbaseDataInformation<>(cl, this.nameSpace);
            HbaseDataInformation.buildHbaseDataInformation(cl, this.hbaseDataInformation);
            this.createTablesIfNeeded(this.hbaseDataInformation);
            this.bufferedMutator = AbstractDAO
                .getBufferedMutator(this.connection, this.hbaseDataInformation.getTable());
        }
    }

    protected void createTablesIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            AbstractDAO.LOGGER.info("Creating table {} and {}", hdi.getTableName(), hdi.getPageTableName());
            AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
            TableName tn = AbstractDAO.createTableIfNeeded(admin, hdi.getTableName(), hdi.getFamilies());
            hdi.setTable(tn);
            if (hdi.getPageTableName() != null) {
                AbstractDAO.LOGGER.info("Creating page table {}", hdi.getPageTableName());
                TableName pageTn = this.createPageTableIfNeeded(admin, hdi.getPageTableName());
                hdi.setPageTable(pageTn);
            }

        }
    }

    protected TableName createPageTableIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
        try (
            Admin admin = this.connection.getAdmin()) {
            return this.createPageTableIfNeeded(admin, hdi.getPageTableName());
        }
    }

    protected static byte[] convert(Long p) {
        byte[] retValue = new byte[8];
        Bytes.putLong(retValue, 0, p);
        return retValue;
    }

    protected TableName createPageTableIfNeeded(final Admin admin, String tableName) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (!admin.tableExists(hbaseTable)) {

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hbaseTable);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_LIST_COLUMN_FAMILY))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY));
            try {
                admin.createTable(builder.build());
                try (
                    Table table = this.connection.getTable(hbaseTable)) {
                    this.initializePageTable(table);
                } catch (Exception e) {
                    AbstractDAO.LOGGER.warn(
                        "Error when creating table {}, table already created {} ",
                        tableName,
                        ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            } catch (TableExistsException e) {
                AbstractDAO.LOGGER.warn(
                    "Error when creating table {}, table already created {} ",
                    tableName,
                    ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                AbstractDAO.LOGGER
                    .warn("Error when creating table {} : {} ", tableName, ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return hbaseTable;
    }

    protected void initializePageTable(Table table) throws IOException {
        byte[] key = AbstractDAO.convert(0L);
        Put put = new Put(key)
            .addColumn(
                AbstractDAO.TABLE_PAGE_INFOS_COLUMN_FAMILY,
                AbstractDAO.TABLE_PAGE_INFOS_COLUMN_NB_OF_ELEMS,
                AbstractDAO.convert(0l))
            .addColumn(
                AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
                AbstractDAO.TABLE_PAGE_DESC_COLUMN_MAX_QUALIFER,
                AbstractDAO.convert(0L))
            .addColumn(
                AbstractDAO.TABLE_PAGE_DESC_COLUMN_FAMILY,
                AbstractDAO.TABLE_PAGE_DESC_COLUMN_MIN_QUALIFER,
                AbstractDAO.convert(Long.MAX_VALUE));
        table.put(put);
    }

    protected static TableName createTableIfNeeded(
        final Admin admin,
        String tableName,
        Collection<String> values,
        byte[] firstRow,
        byte[] lastRow,
        int nbOfRegions
    ) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (!admin.tableExists(hbaseTable)) {

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hbaseTable);

            values.forEach((cfName) -> { builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cfName)); });
            try {
                admin.createTable(builder.build(), firstRow, lastRow, nbOfRegions);
            } catch (TableExistsException e) {
                AbstractDAO.LOGGER.warn(
                    "Error when creating table {}, table already created {} ",
                    tableName,
                    ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                AbstractDAO.LOGGER
                    .warn("Error when creating table {} : {} ", tableName, ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return hbaseTable;
    }

    protected static TableName createTableIfNeeded(final Admin admin, String tableName, Collection<String> values)
        throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        if (!admin.tableExists(hbaseTable)) {

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(hbaseTable);
            builder.setRegionReplication(1);
            values.forEach((cfName) -> {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cfName));

            });
            try {
                admin.createTable(builder.build());
            } catch (TableExistsException e) {
                AbstractDAO.LOGGER.warn(
                    "Error when creating table {}, table already created {} ",
                    tableName,
                    ExceptionUtils.getStackTrace(e));
            } catch (Exception e) {
                AbstractDAO.LOGGER
                    .warn("Error when creating table {} : {} ", tableName, ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return hbaseTable;
    }

    protected static void createNameSpaceIFNeeded(final Admin admin, String nameSpace) throws IOException {
        if (!AbstractDAO.namespaceExists(admin, nameSpace)) {
            try {
                admin.createNamespace(
                    NamespaceDescriptor.create(nameSpace)
                        .build());
            } catch (Exception e) {
                AbstractDAO.LOGGER.warn("Error when creating name space {} ", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
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