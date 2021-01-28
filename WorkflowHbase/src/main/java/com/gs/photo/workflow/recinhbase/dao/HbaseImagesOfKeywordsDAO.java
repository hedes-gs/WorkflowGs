package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfKeywordsDAO;

@Component
public class HbaseImagesOfKeywordsDAO extends AbstractHbaseImagesOfKeywordsDAO implements IHbaseImagesOfKeyWordsDAO {

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseKeywordsDAO) this.hbaseKeywordsDAO).truncate();
    }
}