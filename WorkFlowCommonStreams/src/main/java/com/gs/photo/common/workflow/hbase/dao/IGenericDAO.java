package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import com.workflow.model.HbaseData;

public interface IGenericDAO<T extends HbaseData> {

    public void put(T hbaseData, Class<T> cl) throws IOException;

    public void put(T[] hbaseData, Class<T> cl) throws IOException;

    public void put(Collection<T> hbaseData, Class<T> cl) throws IOException;

    T get(T hbaseData, Class<T> cl) throws IOException;

    void delete(T[] hbaseData, Class<T> cl) throws IOException;

    void delete(T hbaseData, Class<T> cl) throws IOException;

    public void put(Put put) throws IOException;

    public void append(T hbaseData) throws IOException;

    public void delete(Delete del) throws IOException;

    byte[] createKey(T hbi);

    public void put(T hbaseData) throws IOException;

    void append(T hbaseData, String... familiesToInclude);

    void delete(T hbaseData, String family, String column);

    public TableName getTableName();

    void put(T hbaseData, String... familiesToInclude);

    void append(Collection<T> hbaseData) throws IOException;

    void append(Collection<T> hbaseData, String... familiesToInclude);

}
