package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfRatingsDAO;
import com.gs.photo.common.workflow.hbase.dao.IRatingsDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfRatingsDAO extends AbstractHbaseImagesOfRatingsDAO implements IHbaseImagesOfRatingsDAO {
    public HbaseImagesOfRatingsDAO(
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

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseRatingsDAO) this.hbaseRatingsDAO).truncate();
    }

    @Override
    public List<HbaseImageThumbnail> getAllImagesOfMetadata(long ratings) { // TODO Auto-generated method stub
        return super.getAllImagesOfMetadata(ratings, 1, 100000).collectList()
            .block();

    }
}