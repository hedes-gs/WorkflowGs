package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseKeywordsDAO;

@Component
public class HbaseKeywordsDAO extends AbstractHbaseKeywordsDAO implements IHbaseKeywordsDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

}
