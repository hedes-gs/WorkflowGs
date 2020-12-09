package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IRatingsDAO;

public interface IHbaseRatingsDAO extends IRatingsDAO {

    public void truncate() throws IOException;

}
