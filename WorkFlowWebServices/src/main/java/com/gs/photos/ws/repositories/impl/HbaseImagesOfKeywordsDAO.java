package com.gs.photos.ws.repositories.impl;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfKeywordsDAO;
import com.gs.photo.common.workflow.hbase.dao.IKeywordsDAO;

@Component
public class HbaseImagesOfKeywordsDAO extends AbstractHbaseImagesOfKeywordsDAO implements IHbaseImagesOfKeywordsDAO {

    protected HbaseImagesOfKeywordsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IKeywordsDAO hbaseKeywordsDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbaseKeywordsDAO);
    }

}