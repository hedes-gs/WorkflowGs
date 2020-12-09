package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;

import com.workflow.model.HbaseData;

public interface IHbaseMetaDataDAO<T extends HbaseData, T1> {

    long countAll() throws IOException, Throwable;

    long countAll(T metaData) throws IOException, Throwable;

    long countAll(T1 key) throws IOException, Throwable;

    void flush() throws IOException;

}
