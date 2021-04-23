package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseRatingsDAO;
import com.workflow.model.HbaseRatings;

@Component
public class HbaseRatingsDAO extends AbstractHbaseRatingsDAO implements IHbaseRatingsDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    public void delete(HbaseRatings hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}
