package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IKeywordsDAO;

public interface IHbaseKeywordsDAO extends IKeywordsDAO {
    public void truncate() throws IOException;

}
