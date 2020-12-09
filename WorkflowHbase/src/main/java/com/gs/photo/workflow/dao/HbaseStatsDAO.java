package com.gs.photo.workflow.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;

@Component
public class HbaseStatsDAO extends AbstractHbaseStatsDAO<HbaseImageThumbnailKey> implements IHbaseStatsDAO {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HbaseStatsDAO.class);

    @Override
    public void put(HbaseImageThumbnailKey key) { super.put(key, this.getHbaseDataInformation()); }

}
