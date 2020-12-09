package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.HbasePersons;

public abstract class AbstractHbasePersonsDAO extends AbstractMetaDataDAO<HbasePersons, String> implements IPersonsDAO {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractHbasePersonsDAO.class);

    @Override
    protected byte[] createKey(String keyword) throws IOException {
        HbasePersons hbasePersons = HbasePersons.builder()
            .withPerson(keyword)
            .build();
        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(hbasePersons, keyValue);
        return keyValue;
    }

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public long countAll(HbasePersons metaData) throws IOException, Throwable {
        return super.countAll(metaData.getPerson());
    }

}
