package com.gs.photo.workflow.hbase;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;
import com.workflow.model.ToByte;

public class HbaseDataInformation<T extends HbaseData> {
    protected static Logger                      LOGGER    = LoggerFactory.getLogger(HbaseDataInformation.class);

    private final Set<HbaseDataFieldInformation> keyFieldsData;
    private final Set<String>                    columnFamily;
    private final Set<HbaseDataFieldInformation> fieldsData;
    private int                                  keyLength = 0;
    private final String                         tableName;
    private TableName                            table;
    private Class<T>                             hbaseDataClass;
    private final String                         nameSpace;

    public int getKeyLength() { return this.keyLength; }

    public void setKeyLength(int keyLength) { this.keyLength = keyLength; }

    public String getTableName() { return this.tableName; }

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
        String prefix
    ) {
        this.fieldsData = new TreeSet<>();
        this.keyFieldsData = new TreeSet<>();
        this.columnFamily = new TreeSet<>();
        if (cl.getAnnotation(HbaseTableName.class) == null) {
            throw new IllegalArgumentException("Tablename is not set for class " + cl);
        }
        this.tableName = prefix + ":" + cl.getAnnotation(HbaseTableName.class)
            .value();
        this.hbaseDataClass = cl;
        this.nameSpace = prefix;
    }

    public void endOfInit() {
        int offset = 0;
        if (this.keyLength == 0) {
            throw new IllegalArgumentException("Unable to check " + this.hbaseDataClass + " : no row key was defined");
        }
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
                if (valueToConvert == null) {
                    if (!hdfi.field.isAnnotationPresent(org.apache.avro.reflect.Nullable.class)) {
                        throw new IllegalArgumentException(
                            " Value is null for " + this.hbaseDataClass + " / " + hdfi.field);
                    }
                } else {
                    byte[] convertedValue = hdfi.toByte(valueToConvert);
                    String cf = hdfi.columnFamily;
                    ColumnFamily value = cfList.computeIfAbsent(cf, (c) -> new ColumnFamily(c));
                    value.addColumn(hdfi.hbaseName, convertedValue);
                }
            } catch (
                SecurityException |
                IllegalArgumentException |
                IllegalAccessException e) {
                HbaseDataInformation.LOGGER
                    .warn("Error when processing {} : {}", hdfi.field, ExceptionUtils.getStackTrace(e));
            }
        });
        return cfList;

    }

    public void buildKey(T hbaseData, byte[] keyValue) {

        Arrays.fill(keyValue, (byte) 0x20);
        this.keyFieldsData.forEach((hdfi) -> {
            try {
                hdfi.field.setAccessible(true);
                Object valueToConvert = hdfi.field.get(hbaseData);
                byte[] convertedValue = hdfi.toByte(valueToConvert);
                System.arraycopy(convertedValue, 0, keyValue, hdfi.offset, convertedValue.length);
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

    public Collection<String> getFamilies() { return this.columnFamily; }

    public void build(T instance, Result res) {
        byte[] row = res.getRow();
        this.keyFieldsData.forEach((hdfi) -> {
            Object v = null;
            if (hdfi.partOfRowKey) {
                v = hdfi.fromByte(row, hdfi.offset, hdfi.fixedWidth);
            }
            try {
                hdfi.field.set(instance, v);
            } catch (
                IllegalArgumentException |
                IllegalAccessException e) {
                e.printStackTrace();
            }
        });
        this.fieldsData.forEach((hdfi) -> {
            Optional<byte[]> value = Optional
                .ofNullable(res.getValue(hdfi.columnFamily.getBytes(), hdfi.hbaseName.getBytes()));
            value.ifPresent((vByte) -> {
                try {
                    hdfi.field.set(instance, hdfi.fromByte(vByte, 0, vByte.length));
                } catch (
                    IllegalArgumentException |
                    IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    public TableName getTable() { return this.table; }

    public void setTable(TableName table) { this.table = table; }

    public Class<T> getHbaseDataClass() {
        // TODO Auto-generated method stub
        return this.hbaseDataClass;
    }

    public String getNameSpace() { return this.nameSpace; }

    public static <T extends HbaseData> void buildHbaseDataInformation(
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

                        @SuppressWarnings("unchecked")
                        ToByte<Object> toByteInterface = (ToByte<Object>) Proxy.newProxyInstance(
                            Thread.currentThread()
                                .getContextClassLoader(),
                            new Class[] { transformClass },
                            (proxy, method, args) -> {

                                Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class);
                                constructor.setAccessible(true);
                                return constructor.newInstance(transformClass)
                                    .in(transformClass)
                                    .unreflectSpecial(method, transformClass)
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

    @Override
    public String toString() {
        return "HbaseDataInformation [keyFieldsData=" + this.keyFieldsData + ", columnFamily=" + this.columnFamily
            + ", fieldsData=" + this.fieldsData + ", keyLength=" + this.keyLength + ", tableName=" + this.tableName
            + ", table=" + this.table + ", hbaseDataClass=" + this.hbaseDataClass + ", nameSpace=" + this.nameSpace
            + "]";
    }

}
