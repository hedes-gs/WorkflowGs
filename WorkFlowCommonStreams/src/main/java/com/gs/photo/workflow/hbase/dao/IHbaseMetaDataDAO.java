package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;

import com.workflow.model.HbaseData;

public interface IHbaseMetaDataDAO<T extends HbaseData, T1> {

    public void incrementNbOfImages(T metaData) throws IOException;

    public void incrementNbOfImages(T1 metaDataKey) throws IOException;

    void decrementNbOfImages(T metaData) throws IOException;

    void decrementNbOfImages(T1 metaData) throws IOException;

    long countAll() throws IOException, Throwable;

    long countAll(T metaData) throws IOException, Throwable;

    long countAll(T1 key) throws IOException, Throwable;

}
