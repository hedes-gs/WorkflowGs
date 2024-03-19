package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.List;

import com.gs.photo.common.workflow.hbase.dao.IHbaseMetaDataDAO;
import com.workflow.model.HbaseAlbum;

public interface IHbaseAlbumDAO extends IHbaseMetaDataDAO<HbaseAlbum, String> {

    List<HbaseAlbum> getAll() throws IOException;

    @Override
    void flush() throws IOException;

    List<HbaseAlbum> getAllAlbumsLike(String Person) throws IOException;

}