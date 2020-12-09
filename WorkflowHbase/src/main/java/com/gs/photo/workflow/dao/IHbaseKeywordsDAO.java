package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IKeywordsDAO;

public interface IHbaseKeywordsDAO extends IKeywordsDAO {
    public void truncate() throws IOException;

}
