package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfRatingsDAO;

@Component
public class HbaseImagesOfRatingsDAO extends AbstractHbaseImagesOfRatingsDAO implements IHbaseImagesOfRatingsDAO {
    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseRatingsDAO) this.hbaseRatingsDAO).truncate();
    }
}