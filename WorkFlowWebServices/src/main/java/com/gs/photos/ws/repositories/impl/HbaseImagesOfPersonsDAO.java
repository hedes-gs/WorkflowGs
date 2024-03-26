package com.gs.photos.ws.repositories.impl;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfPersonsDAO;
import com.gs.photo.common.workflow.hbase.dao.IPersonsDAO;
import com.workflow.model.dtos.ImageDto;

@Component
public class HbaseImagesOfPersonsDAO extends AbstractHbaseImagesOfPersonsDAO implements IHbaseImagesOfPersonsDAO {

    protected static Logger LOGGER = LoggerFactory.getLogger(HbaseImagesOfPersonsDAO.class);

    @Override
    public void deleteReferences(ImageDto imageToDelete) {}

    protected HbaseImagesOfPersonsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IPersonsDAO hbasePersonDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbasePersonDAO);
    }

}