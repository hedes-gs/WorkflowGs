package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.workflow.hbase.ColumnFamily;
import com.gs.photo.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

public abstract class GenericDAO<T extends HbaseData> extends AbstractDAO<T> implements IGenericDAO<T> {

    static Logger LOGGER = LoggerFactory.getLogger(GenericDAO.class);

    protected void put(Collection<T> hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            List<Put> puts = hbaseData.stream()
                .map((hb) -> {
                    Put put = this.createHbasePut(
                        this.getKey(hb, hbaseDataInformation),
                        this.getCfList(hb, hbaseDataInformation));
                    return put;
                })
                .collect(Collectors.toList());
            table.put(puts);
            puts.clear();
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<T> hbaseData, Class<T> cl) throws IOException {
        this.checkForClass(cl);
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
                List<Put> puts = hbaseData.stream()
                    .map((hb) -> {
                        Put put = this.createHbasePut(
                            this.getKey(hb, hbaseDataInformation),
                            this.getCfList(hb, hbaseDataInformation));
                        return put;
                    })
                    .collect(Collectors.toList());
                table.put(puts);
                puts.clear();
            }
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(T[] hbaseData, Class<T> cl) throws IOException {
        this.checkForClass(cl);
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
                List<Put> puts = Arrays.stream(hbaseData)
                    .map((hb) -> {
                        Put put = this.createHbasePut(
                            this.getKey(hb, hbaseDataInformation),
                            this.getCfList(hb, hbaseDataInformation));
                        return put;
                    })
                    .collect(Collectors.toList());
                table.put(puts);
                puts.clear();
            }
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(T[] hbaseData, Class<T> cl) throws IOException {
        this.checkForClass(cl);
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            List<Delete> dels = Arrays.stream(hbaseData)
                .map((hb) -> {
                    Delete delete = this.createHbaseDelete(this.getKey(hb, hbaseDataInformation));
                    return delete;
                })
                .collect(Collectors.toList());
            table.delete(dels);
            dels.clear();
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to delete some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public void delete(T[] hbaseData, HbaseDataInformation<T> hbaseDataInformation) throws IOException {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            List<Delete> dels = Arrays.stream(hbaseData)
                .map((hb) -> {
                    Delete delete = this.createHbaseDelete(this.getKey(hb, hbaseDataInformation));
                    return delete;
                })
                .collect(Collectors.toList());
            table.delete(dels);
            dels.clear();
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to delete some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public void delete(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) throws IOException {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            Delete delete = this.createHbaseDelete(this.getKey(hbaseData, hbaseDataInformation));
            table.delete(delete);
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to delete some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(T hbaseData, Class<T> cl) throws IOException {
        this.checkForClass(cl);
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            Put put = this.createHbasePut(
                this.getKey(hbaseData, hbaseDataInformation),
                this.getCfList(hbaseData, hbaseDataInformation));
            table.put(put);

        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    public void put(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());

        try {
            Put put = this.createHbasePut(
                this.getKey(hbaseData, hbaseDataInformation),
                this.getCfList(hbaseData, hbaseDataInformation));
            this.bufferedMutator.mutate(put);

        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (byte b : bytes) {
            sb.append(String.format("0x%02X ", b));
        }
        sb.append("]");
        return sb.toString();
    }

    protected Put createHbasePut(byte[] rowKey, Map<String, ColumnFamily> cfList) {
        Put put = new Put(rowKey);
        cfList.entrySet()
            .forEach((cf) -> {
                byte[] bytesOfCfName = cf.getKey()
                    .getBytes();
                cf.getValue()
                    .getValues()
                    .entrySet()
                    .forEach((q) -> { put.addColumn(bytesOfCfName, q.getKey(), q.getValue()); });
            });
        return put;
    }

    protected Put createHbasePut(T hbaseData) throws IOException {
        Put put = new Put(this.getKey(hbaseData));
        return put;
    }

    protected Delete createHbaseDelete(byte[] rowKey) {
        Delete delete = new Delete(rowKey);
        return delete;
    }

    protected byte[] getKey(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {

        byte[] keyValue = new byte[hbaseDataInformation.getKeyLength()];
        hbaseDataInformation.buildKey(hbaseData, keyValue);
        return keyValue;
    }

    protected byte[] getKey(T hbaseData) throws IOException {
        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.hbaseDataInformation.buildKey(hbaseData, keyValue);
        return keyValue;
    }

    protected Map<String, ColumnFamily> getCfList(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        return hbaseDataInformation.buildValue(hbaseData);
    }

    @Override
    public T get(T hbaseData, Class<T> cl) throws IOException {
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        Get get;
        byte[] key = this.getKey(hbaseData, hbaseDataInformation);
        get = new Get(key);
        try {
            try (
                Table table = AbstractDAO.getTable(
                    this.connection,
                    cl.getAnnotation(HbaseTableName.class)
                        .value())) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    T retValue = this.toResult(hbaseDataInformation, result);
                    return retValue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void truncate(HbaseDataInformation<T> hbaseDataInformation) throws IOException {
        GenericDAO.LOGGER.warn("Truncate table {}", hbaseDataInformation.getTable());
        Admin admin = this.connection.getAdmin();
        if (!admin.isTableDisabled(hbaseDataInformation.getTable())) {
            admin.disableTable(hbaseDataInformation.getTable());
        }
        admin.truncateTable(hbaseDataInformation.getTable(), false);
        if (admin.isTableDisabled(hbaseDataInformation.getTable())) {
            admin.enableTable(hbaseDataInformation.getTable());
        }
    }

    public int countWithCoprocessorJob(HbaseDataInformation<T> hbaseDataInformation) throws Throwable {
        try (
            AggregationClient ac = new AggregationClient(HBaseConfiguration.create())) {
            long retValue = ac.rowCount(hbaseDataInformation.getTable(), new LongColumnInterpreter(), new Scan());
            return (int) retValue;
        }
    }

    public T get(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) throws IOException {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        Get get;
        byte[] key = this.getKey(hbaseData, hbaseDataInformation);
        get = new Get(key);
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    T retValue = this.toResult(hbaseDataInformation, result);
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
            T instance = hbaseDataInformation.getHbaseDataClass()
                .newInstance();
            hbaseDataInformation.build(instance, res);

            return instance;
        } catch (
            InstantiationException |
            IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void delete(T hbaseData, Class<T> cl) throws IOException {
        this.checkForClass(cl);
        @SuppressWarnings("unchecked")
        T[] a = (T[]) Array.newInstance(cl, 1);
        a[0] = hbaseData;
        this.delete(a, cl);
    }

    protected void checkForClass(Class<T> cl) {
        Class<?> currentClass = this.getClass();
        Type param = currentClass.getGenericSuperclass();
        while (!(param instanceof ParameterizedType)) {
            currentClass = this.getClass()
                .getSuperclass();
            param = currentClass.getGenericSuperclass();
            if (currentClass == Object.class) { throw new RuntimeException("Unable to find generic type " + this); }
        }
        if (param instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) param;
            Type[] types = paramType.getActualTypeArguments();
            boolean found = false;
            for (Type t : types) {
                if (t instanceof Class<?>) {
                    Class<?> cl1 = (Class<?>) t;
                    if (cl1 == cl) {
                        found = true;
                    }
                }
            }

            if (!found) { throw new IllegalArgumentException(" Class " + cl + " is not authorized..."); }
        }
    }

    @PostConstruct
    protected void init() throws IOException {
        Class<?> currentClass = this.getClass();
        Type param = currentClass.getGenericSuperclass();
        while (!(param instanceof ParameterizedType)) {
            currentClass = this.getClass()
                .getSuperclass();
            param = currentClass.getGenericSuperclass();
            if (currentClass == Object.class) { throw new RuntimeException("Unable to find generic type " + this); }
        }
        if (param instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) param;
            Type[] types = paramType.getActualTypeArguments();
            if (types.length >= 1) {
                for (Type t : types) {
                    if (t instanceof Class<?>) {
                        Class<?> cl = (Class<?>) t;
                        if (HbaseData.class.isAssignableFrom(cl)) {
                            this.createHbaseDataInformation((Class<T>) cl);
                        }
                    }
                }

            }
        }
    }
}
