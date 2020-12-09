package com.gs.photo.workflow.dao;

import java.io.IOException;

import com.gs.photo.workflow.hbase.dao.IImagesOfKeyWordsDAO;

public interface IHbaseImagesOfKeyWordsDAO extends IImagesOfKeyWordsDAO {

    public void truncate() throws IOException;

}
