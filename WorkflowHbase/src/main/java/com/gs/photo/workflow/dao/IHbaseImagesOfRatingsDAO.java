package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImagesOfRatings;

public interface IHbaseImagesOfRatingsDAO extends IHbaseImagesOfMetadataDAO<HbaseImagesOfRatings, Integer> {
    void flush() throws IOException;

    void truncate() throws IOException;

}
