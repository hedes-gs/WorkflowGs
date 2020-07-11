package com.gs.photo.workflow.dao;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractDAO;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;

@Component
public class HbaseStatsDAO extends AbstractHbaseStatsDAO implements IHbaseStatsDAO {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HbaseStatsDAO.class);

    @Override
    public void incrementDateInterval(String key, HbaseImageThumbnailKey imageId) throws IOException {
        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(imageId, keyValue);
        Put imageInfo = new Put(AbstractDAO.toBytes(key))
            .addColumn(AbstractDAO.FAMILY_IMGS_NAME_AS_BYTES, keyValue, AbstractDAO.TRUE_VALUE);
        Increment inc = new Increment(AbstractDAO.toBytes(key))
            .addColumn(AbstractDAO.FAMILY_STATS_NAME_AS_BYTES, AbstractDAO.COLUMN_STAT_AS_BYTES, 1);
        HbaseStatsDAO.LOGGER.info("EVENT[{}] is incrementing {}", imageId.getImageId(), key);
        this.bufferedMutator.mutate(Arrays.asList(imageInfo, inc));
    }

}
