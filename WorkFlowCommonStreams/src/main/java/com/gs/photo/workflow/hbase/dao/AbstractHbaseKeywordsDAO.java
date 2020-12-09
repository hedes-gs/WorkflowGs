package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;

import com.workflow.model.HbaseKeywords;

public abstract class AbstractHbaseKeywordsDAO extends AbstractMetaDataDAO<HbaseKeywords, String>
    implements IKeywordsDAO {

    @Override
    protected byte[] createKey(String keyword) throws IOException {
        HbaseKeywords HbaseKeywords = com.workflow.model.HbaseKeywords.builder()
            .withKeyword(keyword)
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
    public long countAll(HbaseKeywords metaData) throws IOException, Throwable {
        return super.countAll(metaData.getKeyword());
    }

}
