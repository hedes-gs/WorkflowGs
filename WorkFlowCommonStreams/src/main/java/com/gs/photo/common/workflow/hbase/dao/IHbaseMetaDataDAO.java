package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;

import com.workflow.model.HbaseData;

public interface IHbaseMetaDataDAO<T extends HbaseData, T1> {

    long countAll() throws IOException, Throwable;

    long countAll(T metaData) throws IOException, Throwable;

    long countAll(T1 key) throws IOException, Throwable;

    void flush() throws IOException;

    public TableName getTableName();

}
