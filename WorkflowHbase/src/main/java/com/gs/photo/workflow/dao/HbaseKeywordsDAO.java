package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractMetaDataDAO;
import com.workflow.model.HbaseKeywords;

@Component
public class HbaseKeywordsDAO extends AbstractMetaDataDAO<HbaseKeywords, String> implements IHbaseKeywordsDAO {

    @Override
    protected byte[] createKey(String keyword) throws IOException {
        HbaseKeywords HbaseKeywords = com.workflow.model.HbaseKeywords.builder()
            .withKeyword(keyword)
            .build();

        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(HbaseKeywords, keyValue);
        return keyValue;
    }

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public void incrementNbOfImages(HbaseKeywords metaData) throws IOException {
        super.incrementNbOfImages(metaData.getKeyword());
    }

    @Override
    public void decrementNbOfImages(HbaseKeywords metaData) throws IOException {
        super.decrementNbOfImages(metaData.getKeyword());
    }

    @Override
    public long countAll(HbaseKeywords metaData) throws IOException, Throwable {
        return super.countAll(metaData.getKeyword());
    }

}
