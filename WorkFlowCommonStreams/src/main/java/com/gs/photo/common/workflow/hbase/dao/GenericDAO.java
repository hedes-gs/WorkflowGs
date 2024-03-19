package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
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

import com.gs.photo.common.workflow.hbase.ColumnFamily;
import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;

public abstract class GenericDAO<T extends HbaseData> extends AbstractDAO<T> implements IGenericDAO<T> {

    static Logger               LOGGER = LoggerFactory.getLogger(GenericDAO.class);
    protected AggregationClient aggregationClient;

    protected void put(Collection<T> hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            List<Put> puts = hbaseData.stream()
                .map((hb) -> {
                    Put put = this.createHbasePut(
                        GenericDAO.getKey(hb, hbaseDataInformation),
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

    protected void append(Collection<T> hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, hbaseDataInformation.getTable())) {
            List<Append> appends = hbaseData.stream()
                .map((hb) -> {
                    Append append = this.createHbaseAppend(
                        GenericDAO.getKey(hb, hbaseDataInformation),
                        this.getCfList(hb, hbaseDataInformation));
                    return append;
                })
                .map((app) -> {
                    try {
                        table.append(app);
                    } catch (IOException e) {
                        GenericDAO.LOGGER.error("Unable to process {}", app);
                        throw new RuntimeException(e);
                    }
                    return app;
                })
                .collect(Collectors.toList());
            appends.clear();
        } catch (Throwable e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            hbaseData.forEach((t) -> GenericDAO.LOGGER.error(" ----> {} ", t));
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableName getTableName() { return this.getHbaseDataInformation()
        .getTable(); }

    @Override
    public void put(T hbaseData) throws IOException {
        this.put(Collections.singleton(hbaseData), this.getHbaseDataInformation());
    }

    @Override
    public void append(T hbaseData, String... familiesToInclude) {
        this.checkForClass(this.hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            Append append = this.createHbaseAppend(
                GenericDAO.getKey(hbaseData, this.hbaseDataInformation),
                this.getCfList(hbaseData, this.hbaseDataInformation, false, familiesToInclude));
            table.append(append);
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                this.hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(T hbaseData, String... familiesToInclude) {
        this.checkForClass(this.hbaseDataInformation.getHbaseDataClass());
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            Put append = this.createHbasePut(
                GenericDAO.getKey(hbaseData, this.hbaseDataInformation),
                this.getCfList(hbaseData, this.hbaseDataInformation, true, familiesToInclude));
            table.put(append);
        } catch (IOException e) {
            GenericDAO.LOGGER.warn(
                "Unable to record some data in {}, error is {}",
                this.hbaseDataInformation.getTable(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void append(T hbaseData) throws IOException {
        this.append(Collections.singleton(hbaseData), this.getHbaseDataInformation());
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
                            GenericDAO.getKey(hb, hbaseDataInformation),
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
    public void put(Put put) throws IOException {
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            table.put(put);
        }
    }

    @Override
    public void delete(Delete del) throws IOException {
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            table.delete(del);
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
                            GenericDAO.getKey(hb, hbaseDataInformation),
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
                    Delete delete = this.createHbaseDelete(GenericDAO.getKey(hb, hbaseDataInformation));
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
                    Delete delete = this.createHbaseDelete(GenericDAO.getKey(hb, hbaseDataInformation));
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
            GenericDAO.LOGGER.info("Deleting data {} ", hbaseData);
            Delete delete = this.createHbaseDelete(GenericDAO.getKey(hbaseData, hbaseDataInformation));
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
                GenericDAO.getKey(hbaseData, hbaseDataInformation),
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
                GenericDAO.getKey(hbaseData, hbaseDataInformation),
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

                final Map<byte[], byte[]> values = cf.getValue()
                    .getValues();
                values.entrySet()
                    .forEach((q) -> { put.addColumn(bytesOfCfName, q.getKey(), q.getValue()); });
            });
        return put;
    }

    protected Append createHbaseAppend(byte[] rowKey, Map<String, ColumnFamily> cfList) {
        Append append = new Append(rowKey);
        cfList.entrySet()
            .forEach((cf) -> {
                byte[] bytesOfCfName = cf.getKey()
                    .getBytes();
                cf.getValue()
                    .getValues()
                    .entrySet()
                    .forEach((q) -> { append.addColumn(bytesOfCfName, q.getKey(), q.getValue()); });
            });
        return append;
    }

    protected Put createHbasePut(T hbaseData) throws IOException {
        Put put = new Put(this.getKey(hbaseData));
        return put;
    }

    protected Delete createHbaseDelete(byte[] rowKey) {
        Delete delete = new Delete(rowKey);
        return delete;
    }

    protected static <T1 extends HbaseData> byte[] getKey(T1 hbaseData, HbaseDataInformation<T1> hbaseDataInformation) {
        byte[] keyValue = new byte[hbaseDataInformation.getKeyLength()];
        hbaseDataInformation.buildKey(hbaseData, keyValue);
        return keyValue;
    }

    protected Map<String, ColumnFamily> getCfList(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) {
        return hbaseDataInformation.buildValue(hbaseData, false);
    }

    protected Map<String, ColumnFamily> getCfList(
        T hbaseData,
        HbaseDataInformation<T> hbaseDataInformation,
        boolean ignoreNullValue,
        String... families
    ) {
        List<String> familiesToInclude = Arrays.asList(families);
        return hbaseDataInformation.buildValue(hbaseData, ignoreNullValue)
            .entrySet()
            .stream()
            .filter(((e) -> familiesToInclude.contains(e.getKey())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public byte[] createKey(T hbi) { return this.getHbaseDataInformation()
        .buildKey(hbi); }

    @Override
    public T get(T hbaseData, Class<T> cl) throws IOException {
        HbaseDataInformation<T> hbaseDataInformation = this.getHbaseDataInformation();
        Get get;
        byte[] key = GenericDAO.getKey(hbaseData, hbaseDataInformation);
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
            if (admin.isTableEnabled(hbaseDataInformation.getTable())) {
                admin.disableTable(hbaseDataInformation.getTable());
            }
        }
        admin.truncateTable(hbaseDataInformation.getTable(), false);
        if (admin.isTableDisabled(hbaseDataInformation.getTable())) {
            admin.enableTable(hbaseDataInformation.getTable());
        }
        if (hbaseDataInformation.getPageTable() != null) {
            if (!admin.isTableDisabled(hbaseDataInformation.getPageTable())) {
                if (admin.isTableEnabled(hbaseDataInformation.getPageTable())) {
                    admin.disableTable(hbaseDataInformation.getPageTable());
                }
            }
            admin.truncateTable(hbaseDataInformation.getPageTable(), false);
            if (admin.isTableDisabled(hbaseDataInformation.getPageTable())) {
                admin.enableTable(hbaseDataInformation.getPageTable());
            }
            try (
                Table table = this.connection.getTable(hbaseDataInformation.getPageTable())) {
                this.initializePageTable(table);
            } catch (Exception e) {
                AbstractDAO.LOGGER.warn(
                    "Error when creating table {}, table already created {} ",
                    hbaseDataInformation.getPageTable(),
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }

    }

    public Long countWithCoprocessorJob(HbaseDataInformation<T> hbaseDataInformation) throws Throwable {
        try (
            AggregationClient ac = new AggregationClient(HBaseConfiguration.create())) {
            long retValue = ac.rowCount(hbaseDataInformation.getTable(), new LongColumnInterpreter(), new Scan());
            return retValue;
        }
    }

    public T get(T hbaseData, HbaseDataInformation<T> hbaseDataInformation) throws IOException {
        this.checkForClass(hbaseDataInformation.getHbaseDataClass());
        Get get;
        byte[] key = GenericDAO.getKey(hbaseData, hbaseDataInformation);
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

    public byte[] getKey(T hbi) {
        byte[] key = GenericDAO.getKey(hbi, this.hbaseDataInformation);
        return key;
    }

    public T get(byte[] key) {
        Get get = new Get(key);
        try {
            try (
                Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
                Result result = table.get(get);
                if ((result != null) && !result.isEmpty()) {
                    T retValue = this.toResult(this.hbaseDataInformation, result);
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
                .getDeclaredConstructor()
                .newInstance();
            hbaseDataInformation.build(instance, res);

            return instance;
        } catch (
            InstantiationException |
            IllegalAccessException |
            IllegalArgumentException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
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
        GenericDAO.LOGGER.info(
            "In init of {} ",
            this.getClass()
                .getSimpleName());
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

    protected GenericDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }
}
