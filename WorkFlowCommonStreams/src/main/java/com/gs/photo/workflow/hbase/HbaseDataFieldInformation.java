package com.gs.photo.workflow.hbase;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

import com.workflow.model.Column;
import com.workflow.model.ToByte;

public class HbaseDataFieldInformation implements Comparable<HbaseDataFieldInformation> {
    protected Field                           field;
    protected Class<? extends ToByte<Object>> transformClass;
    protected ToByte<Object>                  transformInstance;
    protected Column                          column;
    protected boolean                         partOfRowKey;
    protected int                             fixedWidth;
    protected int                             rowKeyNumber;
    protected String                          columnFamily;
    protected String                          hbaseName;
    public int                                offset;

    public byte[] toByte(Object valueToConvert) {
        byte[] convertedValue = this.transformInstance.convert(valueToConvert);
        return convertedValue;
    }

    public Object fromByte(byte[] buffer, int offset, int length) {
        Object value = this.transformInstance.fromByte(buffer, offset, length);
        return value;
    }

    @SuppressWarnings("unchecked")
    public HbaseDataFieldInformation(
        Field field,
        Class<? extends ToByte<Object>> transformClass,
        Column column
    ) {
        super();
        this.field = field;
        this.transformClass = transformClass;
        this.column = column;
        this.partOfRowKey = this.column.isPartOfRowkey();
        this.fixedWidth = this.column.fixedWidth();
        this.rowKeyNumber = column.rowKeyNumber();
        this.columnFamily = column.columnFamily();
        this.hbaseName = column.hbaseName();
        this.transformInstance = ((ToByte<Object>) Proxy.newProxyInstance(
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

            })).getInstance();
    }

    @Override
    public int compareTo(HbaseDataFieldInformation o) { return Integer.compare(this.rowKeyNumber, o.rowKeyNumber); }

}