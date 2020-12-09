package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IImagesOfAlbumDAO;

public interface IHbaseImagesOfAlbumDAO extends IImagesOfAlbumDAO {

    void truncate() throws IOException;

}
