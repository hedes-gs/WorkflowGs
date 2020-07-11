package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractMetaDataDAO;
import com.workflow.model.HbaseRatings;

@Component
public class HbaseRatingsDAO extends AbstractMetaDataDAO<HbaseRatings, Integer> implements IHbaseRatingsDAO {

    @Override
    protected byte[] createKey(Integer rating) throws IOException {
        HbaseRatings HbaseKeywords = com.workflow.model.HbaseRatings.builder()
            .withRatings(rating)
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
    public void incrementNbOfImages(HbaseRatings metaData) throws IOException {
        super.incrementNbOfImages(metaData.getRatings());
    }

    @Override
    public void decrementNbOfImages(HbaseRatings metaData) throws IOException {
        super.decrementNbOfImages(metaData.getRatings());
    }

    @Override
    public long countAll(HbaseRatings metaData) throws IOException, Throwable {
        return super.countAll(metaData.getRatings());
    }

}
