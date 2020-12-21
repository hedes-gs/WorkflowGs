package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.List;

import com.gs.photo.workflow.hbase.dao.IHbaseMetaDataDAO;
import com.workflow.model.HbasePersons;

public interface IHbasePersonsDAO extends IHbaseMetaDataDAO<HbasePersons, String> {

    List<HbasePersons> getAll() throws IOException;

    void flush() throws IOException;

    List<HbasePersons> getAllPersonsLike(String Person) throws IOException;

}
