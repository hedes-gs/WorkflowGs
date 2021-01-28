package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfAlbumDAO;

public interface IHbaseImagesOfAlbumDAO extends IImagesOfAlbumDAO {

    void truncate() throws IOException;

}
