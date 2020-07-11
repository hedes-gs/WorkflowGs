package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseMetaDataDAO;
import com.workflow.model.HbaseRatings;

public interface IHbaseRatingsDAO extends IHbaseMetaDataDAO<HbaseRatings, Integer> {

    public void flush() throws IOException;

    public void truncate() throws IOException;

}
