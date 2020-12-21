package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IPersonsDAO;

public interface IHbasePersonsDAO extends IPersonsDAO {

    public void truncate() throws IOException;
}
