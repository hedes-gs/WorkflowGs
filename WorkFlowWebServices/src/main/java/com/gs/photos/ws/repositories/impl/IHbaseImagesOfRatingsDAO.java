package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.Map;

import com.gs.photo.common.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;

public interface IHbaseImagesOfRatingsDAO extends IHbaseImagesOfMetadataDAO<HbaseImageThumbnail, Long> {
    @Override
    void flush() throws IOException;

    Map<String, Long> countAllPerRatings() throws IOException, Throwable;

}
