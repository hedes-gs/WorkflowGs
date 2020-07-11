package com.gs.photos.repositories.impl;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfKeywords;

public interface IHbaseImagesOfKeywordsDAO extends IHbaseImagesOfMetadataDAO<HbaseImagesOfKeywords, String> {
    void flush() throws IOException;

    void removeFromMetadata(HbaseImageThumbnail retValue, String keyword) throws IOException;

}
