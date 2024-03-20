package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfAlbumsDAO;
import com.gs.photo.common.workflow.hbase.dao.IAlbumDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfAlbumDAO extends AbstractHbaseImagesOfAlbumsDAO implements IHbaseImagesOfAlbumDAO {

    public HbaseImagesOfAlbumDAO(
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

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseAlbumDAO) this.hbaseAlbumDAO).truncate();
    }

    @Override
    public List<HbaseImageThumbnail> getAllImagesOfMetadata(String album) {
        return super.getAllImagesOfMetadata(album, 1, 100000).collectList()
            .block();
    }

}
