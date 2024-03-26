package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfRatingsDAO;
import com.gs.photo.common.workflow.hbase.dao.IRatingsDAO;

@Component
public class HbaseImagesOfRatingsDAO extends AbstractHbaseImagesOfRatingsDAO implements IHbaseImagesOfRatingsDAO {

    @Override
    public Map<String, Long> countAllPerRatings() throws IOException, Throwable { return null; }

    protected HbaseImagesOfRatingsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IRatingsDAO hbaseRatingsDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbaseRatingsDAO);
    }

}