package com.gs.photo.common.workflow.hbase.dao;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImagesOfMetadata;

public abstract class AbstractHbaseImagesOfStringMetadata<T extends HbaseImagesOfMetadata> extends
    AbstractHbaseImagesOfMetadataDAO<T, String> implements IHbaseImagesOfMetadataDAO<HbaseImageThumbnail, String> {

    protected static Logger      LOGGER = LoggerFactory.getLogger(AbstractHbaseImagesOfStringMetadata.class);

    protected IImageThumbnailDAO hbaseImageThumbnailDAO;

    public AbstractHbaseImagesOfStringMetadata(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO
    ) {
        super(connection,
            nameSpace);
        this.hbaseImageThumbnailDAO = hbaseImageThumbnailDAO;
    }

}