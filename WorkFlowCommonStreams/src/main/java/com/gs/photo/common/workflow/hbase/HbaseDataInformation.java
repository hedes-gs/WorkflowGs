package com.gs.photo.common.workflow.hbase;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.workflow.model.Column;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseTableName;
import com.workflow.model.ToByte;

public class HbaseDataInformation<T extends HbaseData> {
    public static final byte[]                   TRUE_VALUE = new byte[] { 1 };

    protected static Logger                      LOGGER     = LoggerFactory.getLogger(HbaseDataInformation.class);

    private final Set<HbaseDataFieldInformation> keyFieldsData;
    private final Set<String>                    columnFamily;
    private final Set<HbaseDataFieldInformation> fieldsData;
    private int                                  keyLength  = 0;
    private final String                         tableName;
    private TableName                            table;
    private String                               pageTableName;
    private TableName                            pageTable;
    private Class<T>                             hbaseDataClass;
    private final String                         nameSpace;

    public int getKeyLength() { return this.keyLength; }

    public byte[] getMinKey() {
        byte[] retValue = new byte[this.getKeyLength()];
        Arrays.fill(retValue, (byte) 0);
        return retValue;
    }

    public byte[] getMaxKey() {
        byte[] retValue = new byte[this.getKeyLength()];
        Arrays.fill(retValue, (byte) 0xff);
        return retValue;
    }

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
        this.pageTableName = cl.getAnnotation(HbaseTableName.class)
            .page_table()
                ? prefix + ":page_" + cl.getAnnotation(HbaseTableName.class)
                    .value()
                : null;
        this.hbaseDataClass = cl;
        this.nameSpace = prefix;
    }

    public T newInstance() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {
        return this.hbaseDataClass.getDeclaredConstructor()
            .newInstance();
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
                    String cf = hdfi.columnFamily;
                    ColumnFamily value = cfList.computeIfAbsent(cf, (c) -> new ColumnFamily(c));
                    if (Set.class.isAssignableFrom(hdfi.field.getType())) {
                        Set<?> collection = (Set<?>) valueToConvert;
                        collection.forEach((a) -> value.addColumn(hdfi.toByte(a), HbaseDataInformation.TRUE_VALUE));
                    } else if (Map.class.isAssignableFrom(hdfi.field.getType())) {
                        Map<?, ?> collection = (Map<?, ?>) valueToConvert;
                        collection.entrySet()
                            .forEach(
                                (a) -> value.addColumn(
                                    a.getKey()
                                        .toString(),
                                    hdfi.toByte(a.getValue())));
                    } else {
                        byte[] convertedValue = hdfi.toByte(valueToConvert);
                        value.addColumn(hdfi.hbaseName, convertedValue);
                    }
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

    public byte[] buildKey(T hbaseData) {
        byte[] retValue = new byte[this.getKeyLength()];
        this.buildKey(hbaseData, retValue);
        return retValue;
    }

    public T buildKey(T hbaseData, byte[] keyValue) {

        Arrays.fill(keyValue, (byte) 0x00);
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
        return hbaseData;
    }

    public T buildKeyFromRowKey(T instance, byte[] row) {

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
        return instance;
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
            if (Set.class.isAssignableFrom(hdfi.field.getType())) {
                Set<?> collection = new HashSet<>();
                for (Cell statCell : res.listCells()) {
                    if (hdfi.columnFamily.equals(new String(CellUtil.cloneFamily(statCell)))) {
                        try {
                            collection.add(hdfi.fromByte(CellUtil.cloneQualifier(statCell)));
                        } catch (Exception e) {
                            HbaseDataInformation.LOGGER.warn(
                                "Unable to add to collection of family {} : {} ",
                                hdfi.columnFamily,
                                CellUtil.cloneQualifier(statCell));
                            throw new RuntimeException(e);
                        }
                    }
                }
                try {
                    hdfi.field.set(instance, collection);
                } catch (
                    IllegalArgumentException |
                    IllegalAccessException e) {
                    e.printStackTrace();
                }
            } else if (Map.class.isAssignableFrom(hdfi.field.getType())) {
                Map<Object, ?> collection = new HashMap<>();
                for (Cell statCell : res.listCells()) {
                    if (hdfi.columnFamily.equals(
                        new String(statCell.getFamilyArray(),
                            statCell.getFamilyOffset(),
                            statCell.getFamilyLength()))) {
                        Object key = null;
                        if (hdfi.column.mapKeyClass() == Integer.class) {
                            key = Integer.parseInt(new String(CellUtil.cloneQualifier(statCell)));
                        } else {
                            key = new String(CellUtil.cloneQualifier(statCell));
                        }
                        if (!StringUtils.isEmpty(hdfi.column.mapOnlyOneElement())) {
                            if (Objects.equal(hdfi.column.mapOnlyOneElement(), key)) {
                                collection.put(key, hdfi.fromByte(CellUtil.cloneValue(statCell)));
                            }
                        } else {
                            collection.put(key, hdfi.fromByte(CellUtil.cloneValue(statCell)));
                        }

                    }
                }
                try {
                    hdfi.field.set(instance, collection);
                } catch (
                    IllegalArgumentException |
                    IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
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
            }
        });
    }

    public T buildOnlyKey(T instance, byte[] row) {
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
        return instance;
    }

    public TableName getTable() { return this.table; }

    public void setTable(TableName table) { this.table = table; }

    public TableName getPageTable() { return this.pageTable; }

    public void setPageTable(TableName pageTable) { this.pageTable = pageTable; }

    public String getPageTableName() { return this.pageTableName; }

    public void setPageTableName(String pageTableName) { this.pageTableName = pageTableName; }

    public Class<T> getHbaseDataClass() {
        // TODO Auto-generated method stub
        return this.hbaseDataClass;
    }

    public String getNameSpace() { return this.nameSpace; }

    public static <T extends HbaseData> void buildHbaseDataInformation(
        Class<T> cl,
        HbaseDataInformation<T> hbaseDataInformation
    ) {
        HbaseDataInformation.getAllFieldsAnnotatedWithColumn(new ArrayList<>(), cl)
            .forEach((field) -> {
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
            });
        hbaseDataInformation.endOfInit();
    }

    protected static List<Field> getAllFieldsAnnotatedWithColumn(List<Field> currentFieldsList, Class<?> cl) {
        HbaseDataInformation.LOGGER.info("Examining class {} for fields ", cl);
        for (Field f : cl.getDeclaredFields()) {
            if (f.isAnnotationPresent(Column.class)) {
                currentFieldsList.add(f);
            }
        }
        if (cl.getSuperclass() != Object.class) {
            return HbaseDataInformation.getAllFieldsAnnotatedWithColumn(currentFieldsList, cl.getSuperclass());
        }
        return currentFieldsList;
    }

    @Override
    public String toString() {
        return "HbaseDataInformation [keyFieldsData=" + this.keyFieldsData + ", columnFamily=" + this.columnFamily
            + ", fieldsData=" + this.fieldsData + ", keyLength=" + this.keyLength + ", tableName=" + this.tableName
            + ", table=" + this.table + ", hbaseDataClass=" + this.hbaseDataClass + ", nameSpace=" + this.nameSpace
            + "]";
    }

}
