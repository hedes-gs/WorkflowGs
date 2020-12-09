package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseRatingsDAO;

@Component
public class HbaseRatingsDAO extends AbstractHbaseRatingsDAO implements IHbaseRatingsDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

}
