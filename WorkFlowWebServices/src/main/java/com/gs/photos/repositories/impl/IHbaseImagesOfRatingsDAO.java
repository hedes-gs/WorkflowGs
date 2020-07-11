package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.Map;

import com.gs.photo.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImagesOfRatings;

public interface IHbaseImagesOfRatingsDAO extends IHbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Integer> {
    void flush() throws IOException;

    Map<String, Integer> countAllPerRatings() throws IOException, Throwable;

}
