package com.gs.photos.repositories.impl;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseMetaDataDAO;
import com.workflow.model.HbaseRatings;

public interface IHbaseRatingsDAO extends IHbaseMetaDataDAO<HbaseRatings, Long> {

    void flush() throws IOException;
}
