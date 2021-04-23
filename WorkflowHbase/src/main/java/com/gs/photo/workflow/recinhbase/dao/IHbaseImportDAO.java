package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IImportDAO;

public interface IHbaseImportDAO extends IImportDAO {

    public void truncate() throws IOException;

}