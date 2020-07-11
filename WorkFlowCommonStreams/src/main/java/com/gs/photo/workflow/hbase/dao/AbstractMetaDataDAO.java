package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseData;

public abstract class AbstractMetaDataDAO<T extends HbaseData, T2> extends GenericDAO<T>
    implements IHbaseMetaDataDAO<T, T2> {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractMetaDataDAO.class);

    protected abstract byte[] createKey(T2 key) throws IOException;

    @Override
    public void incrementNbOfImages(T2 key) throws IOException {
        byte[] keyValue = this.createKey(key);

        Put imageInfo = new Put(keyValue)
            .addColumn(AbstractDAO.FAMILY_IMGS_NAME_AS_BYTES, keyValue, AbstractDAO.TRUE_VALUE);
        Increment inc = new Increment(keyValue)
            .addColumn(AbstractDAO.FAMILY_STATS_NAME_AS_BYTES, AbstractDAO.COLUMN_STAT_AS_BYTES, 1);
        AbstractMetaDataDAO.LOGGER
            .info("[METADATA][{}] is incrementing, table is {}", key, this.bufferedMutator.getName());
        this.bufferedMutator.mutate(Arrays.asList(imageInfo, inc));
        this.flush();
    }

    @Override
    public void decrementNbOfImages(T2 key) throws IOException {
        byte[] keyValue = this.createKey(key);

        Put imageInfo = new Put(keyValue)
            .addColumn(AbstractDAO.FAMILY_IMGS_NAME_AS_BYTES, keyValue, AbstractDAO.TRUE_VALUE);
        Increment inc = new Increment(keyValue)
            .addColumn(AbstractDAO.FAMILY_STATS_NAME_AS_BYTES, AbstractDAO.COLUMN_STAT_AS_BYTES, -1);
        AbstractMetaDataDAO.LOGGER
            .info("[METADATA][{}] is decrementing, table is {}", key, this.bufferedMutator.getName());
        this.bufferedMutator.mutate(Arrays.asList(imageInfo, inc));
        this.flush();
    }

    @Override
    public long countAll(T2 key) throws IOException, Throwable {
        byte[] keyValue = this.createKey(key);
        Get get = new Get(keyValue);
        get.addColumn(AbstractDAO.FAMILY_STATS_NAME_AS_BYTES, AbstractDAO.COLUMN_STAT_AS_BYTES);
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    long retValue = Bytes.toLong(
                        CellUtil.cloneValue(
                            result.getColumnLatestCell(
                                AbstractDAO.FAMILY_STATS_NAME_AS_BYTES,
                                AbstractDAO.COLUMN_STAT_AS_BYTES)));

                    return retValue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;

    }

    @Override
    protected TableName createTableIfNeeded(HbaseDataInformation<T> hdi) throws IOException {
        Admin admin = this.connection.getAdmin();
        AbstractDAO.createNameSpaceIFNeeded(admin, hdi.getNameSpace());
        return AbstractDAO.createTableIfNeeded(
            admin,
            hdi.getTableName(),
            Arrays.asList(AbstractDAO.FAMILY_IMGS_NAME, AbstractDAO.FAMILY_STATS_NAME));
    }

    public List<T> getAll() throws IOException {
        List<T> retValue = new ArrayList<>();
        HbaseDataInformation<T> hbaseDataInformation = super.getHbaseDataInformation();
        AbstractMetaDataDAO.LOGGER.info("Get All for {} ", hbaseDataInformation.getHbaseDataClass());

        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            Scan scan = new Scan();
            try (
                ResultScanner rscanner = table.getScanner(scan)) {

                for (Result r : rscanner) {
                    T instance = hbaseDataInformation.getHbaseDataClass()
                        .getDeclaredConstructor()
                        .newInstance();
                    hbaseDataInformation.build(instance, r);
                    AbstractMetaDataDAO.LOGGER
                        .info("Get All for {}, found : {} ", hbaseDataInformation.getHbaseDataClass(), instance);
                    retValue.add(instance);
                }
            } catch (
                InstantiationException |
                IllegalAccessException |
                IllegalArgumentException |
                InvocationTargetException |
                NoSuchMethodException |
                SecurityException e) {
                e.printStackTrace();
            }
        }
        return retValue;
    }

}
