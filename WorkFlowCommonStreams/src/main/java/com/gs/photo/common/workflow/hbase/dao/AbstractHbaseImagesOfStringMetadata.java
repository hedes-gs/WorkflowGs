package com.gs.photo.common.workflow.hbase.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfMetadata;

public abstract class AbstractHbaseImagesOfStringMetadata<T extends HbaseImagesOfMetadata> extends
    AbstractHbaseImagesOfMetadataDAO<T, String> implements IHbaseImagesOfMetadataDAO<HbaseImageThumbnail, String> {

    protected static Logger      LOGGER = LoggerFactory.getLogger(AbstractHbaseImagesOfStringMetadata.class);

    @Autowired
    protected IImageThumbnailDAO hbaseImageThumbnailDAO;

}