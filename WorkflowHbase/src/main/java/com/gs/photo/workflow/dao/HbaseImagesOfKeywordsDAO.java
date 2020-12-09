package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseImagesOfKeywordsDAO;

@Component
public class HbaseImagesOfKeywordsDAO extends AbstractHbaseImagesOfKeywordsDAO implements IHbaseImagesOfKeyWordsDAO {

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseKeywordsDAO) this.hbaseKeywordsDAO).truncate();
    }
}