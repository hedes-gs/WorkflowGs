package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfRatingsDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfRatingsDAO extends AbstractHbaseImagesOfRatingsDAO implements IHbaseImagesOfRatingsDAO {
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