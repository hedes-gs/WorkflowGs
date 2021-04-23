package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import com.gs.photo.common.workflow.hbase.dao.IImagesOfKeyWordsDAO;
import com.workflow.model.HbaseImageThumbnail;

public interface IHbaseImagesOfKeyWordsDAO extends IImagesOfKeyWordsDAO {

    public List<HbaseImageThumbnail> getAllImagesOfMetadata(String keyword);

    public void truncate() throws IOException;

}
