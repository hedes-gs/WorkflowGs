package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IHbaseImagesOfMetadataDAO;
import com.workflow.model.HbaseImagesOfKeywords;

public interface IHbaseImagesOfKeyWordsDAO extends IHbaseImagesOfMetadataDAO<HbaseImagesOfKeywords, String> {

    void flush() throws IOException;

    public void truncate() throws IOException;

}
