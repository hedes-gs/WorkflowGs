package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfRatingsDAO;
import com.workflow.model.HbaseImageThumbnail;

public interface IHbaseImagesOfRatingsDAO extends IImagesOfRatingsDAO {

    public List<HbaseImageThumbnail> getAllImagesOfMetadata(long ratings);

    void truncate() throws IOException;
}
