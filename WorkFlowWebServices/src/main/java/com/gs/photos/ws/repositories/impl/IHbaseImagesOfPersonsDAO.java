package com.gs.photos.ws.repositories.impl;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.dtos.ImageDto;

public interface IHbaseImagesOfPersonsDAO extends IHbaseImagesOfMetadataDAO<HbaseImageThumbnail, String> {
    @Override
    void flush() throws IOException;

    void deleteReferences(ImageDto imageToDelete);

}
