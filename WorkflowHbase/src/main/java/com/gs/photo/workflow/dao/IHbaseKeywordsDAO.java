package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseMetaDataDAO;
import com.workflow.model.HbaseKeywords;

public interface IHbaseKeywordsDAO extends IHbaseMetaDataDAO<HbaseKeywords, String> {
    public void flush() throws IOException;

    public void truncate() throws IOException;

}
