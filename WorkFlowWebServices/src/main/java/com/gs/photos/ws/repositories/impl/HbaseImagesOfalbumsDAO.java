package com.gs.photos.ws.repositories.impl;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfAlbumsDAO;
import com.gs.photo.common.workflow.hbase.dao.IAlbumDAO;
import com.workflow.model.HbaseAlbum;
import com.workflow.model.HbaseImagesOfAlbum;

import reactor.core.publisher.Flux;

@Component
public class HbaseImagesOfalbumsDAO extends AbstractHbaseImagesOfAlbumsDAO implements IHbaseImagesOfAlbumsDAO {

    protected static Logger LOGGER = LoggerFactory.getLogger(HbaseImagesOfalbumsDAO.class);

    @Override
    public Flux<HbaseImagesOfAlbum> getPage(HbaseAlbum metadata, int pageNumber, int pageSize) { // TODO Auto-generated
                                                                                                 // method stub
        return null;
    }

    protected HbaseImagesOfalbumsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IAlbumDAO hbaseAlbumDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbaseAlbumDAO);
    }

}