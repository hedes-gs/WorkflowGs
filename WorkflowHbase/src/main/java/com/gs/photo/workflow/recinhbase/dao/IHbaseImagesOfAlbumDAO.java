package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfAlbumDAO;
import com.workflow.model.HbaseImageThumbnail;

public interface IHbaseImagesOfAlbumDAO extends IImagesOfAlbumDAO {

    public List<HbaseImageThumbnail> getAllImagesOfMetadata(String album);

    void truncate() throws IOException;

}
