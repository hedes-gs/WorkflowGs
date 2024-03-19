package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

import com.workflow.model.HbaseRatings;

public abstract class AbstractHbaseRatingsDAO extends AbstractMetaDataDAO<HbaseRatings, Long> implements IRatingsDAO {

    @Override
    protected byte[] createKey(Long rating) throws IOException {
        HbaseRatings HbaseKeywords = com.workflow.model.HbaseRatings.builder()
            .withRatings(rating)
            .build();
        byte[] keyValue = this.getHbaseDataInformation()
            .buildKey(HbaseKeywords);
        return keyValue;
    }

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public long countAll(HbaseRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatings());
    }

    protected AbstractHbaseRatingsDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
