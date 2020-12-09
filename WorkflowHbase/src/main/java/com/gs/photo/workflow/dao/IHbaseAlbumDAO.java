package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IAlbumDAO;

public interface IHbaseAlbumDAO extends IAlbumDAO {

    public void truncate() throws IOException;

}