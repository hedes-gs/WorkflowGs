package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImagesOfAlbum;

public interface IHbaseImagesOfAlbumDAO extends IHbaseImagesOfMetadataDAO<HbaseImagesOfAlbum, String> {

    void flush() throws IOException;

    void truncate() throws IOException;

}
