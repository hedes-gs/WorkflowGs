package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IPersonsDAO;

public interface IHbasePersonsDAO extends IPersonsDAO {

    public void truncate() throws IOException;
}
