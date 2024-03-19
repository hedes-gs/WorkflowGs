package com.gs.photo.workflow.recinhbase.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;

@Component
public class HbaseStatsDAO extends AbstractHbaseStatsDAO<HbaseImageThumbnailKey> implements IHbaseStatsDAO {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HbaseStatsDAO.class);

    @Override
    public void put(HbaseImageThumbnailKey key) { super.put(key, this.getHbaseDataInformation()); }

    @Override
    public void delete(HbaseImageThumbnailKey hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                         // stub
    }

}
