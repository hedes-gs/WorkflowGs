package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseKeywordsDAO;
import com.workflow.model.HbaseKeywords;

@Component
public class HbaseKeywordsDAO extends AbstractHbaseKeywordsDAO implements IHbaseKeywordsDAO {

    protected HbaseKeywordsDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    public void delete(HbaseKeywords hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}
