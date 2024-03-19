package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfPersonsDAO;

@Component
public class HbaseImagesOfPersonsDAO extends AbstractHbaseImagesOfPersonsDAO implements IHbaseImagesOfPersonsDAO {
    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbasePersonsDAO) this.hbasePersonDAO).truncate();
    }
}