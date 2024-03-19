package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IRatingsDAO;

public interface IHbaseRatingsDAO extends IRatingsDAO {

    public void truncate() throws IOException;

}
