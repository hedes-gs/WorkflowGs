package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseData;

public abstract class AbstractMetaDataDAO<T extends HbaseData, T2> extends GenericDAO<T>
    implements IHbaseMetaDataDAO<T, T2> {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractMetaDataDAO.class);

    protected abstract byte[] createKey(T2 key) throws IOException;

    @Override
    public long countAll(T2 key) throws IOException, Throwable {
        byte[] keyValue = this.createKey(key);
        Get get = new Get(keyValue);
        get.addColumn(AbstractDAO.FAMILY_INFOS_NAME_AS_BYTES, AbstractDAO.FAMILY_INFOS_COL_NB_OF_ELEM_AS_BYTES);
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    long retValue = Bytes.toLong(
                        CellUtil.cloneValue(
                            result.getColumnLatestCell(
                                AbstractDAO.FAMILY_INFOS_NAME_AS_BYTES,
                                AbstractDAO.FAMILY_INFOS_COL_NB_OF_ELEM_AS_BYTES)));

                    return retValue;
                }
            }
        } catch (IOException e) {
            AbstractMetaDataDAO.LOGGER
                .error("ERROR detected when count all elements of {} : {} ", key, ExceptionUtils.getStackTrace(e));
            throw e;
        }
        return 0;

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
