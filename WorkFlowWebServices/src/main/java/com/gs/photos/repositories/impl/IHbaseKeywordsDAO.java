package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.util.List;

import com.workflow.model.HbaseKeywords;

public interface IHbaseKeywordsDAO extends IKeywordsDAO {

    List<HbaseKeywords> getAll() throws IOException;

    void flush() throws IOException;

    List<HbaseKeywords> getAllKeywordsLike(String keyword) throws IOException;

}
