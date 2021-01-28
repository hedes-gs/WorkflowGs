package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IAlbumDAO;

public interface IHbaseAlbumDAO extends IAlbumDAO {

    public void truncate() throws IOException;

}