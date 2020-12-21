package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbasePersonsDAO;

@Component
public class HbasePersonsDAO extends AbstractHbasePersonsDAO implements IHbasePersonsDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

}
