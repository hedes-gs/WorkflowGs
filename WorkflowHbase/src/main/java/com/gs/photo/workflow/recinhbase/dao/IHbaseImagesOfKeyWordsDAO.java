package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfKeyWordsDAO;

public interface IHbaseImagesOfKeyWordsDAO extends IImagesOfKeyWordsDAO {

    public void truncate() throws IOException;

}
