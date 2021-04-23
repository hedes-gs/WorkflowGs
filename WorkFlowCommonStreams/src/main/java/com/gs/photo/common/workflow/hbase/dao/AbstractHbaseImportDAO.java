package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;

import com.workflow.model.HbaseImport;

public abstract class AbstractHbaseImportDAO extends AbstractMetaDataDAO<HbaseImport, String> implements IImportDAO {

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public long countAll(HbaseImport metaData) throws IOException, Throwable {
        return super.countAll(metaData.getimportName());
    }

    @Override
    protected byte[] createKey(String album) throws IOException {
        HbaseImport hbaseImport = HbaseImport.builder()
            .withImportName(album)
            .build();
        byte[] keyValue = this.getHbaseDataInformation()
            .buildKey(hbaseImport);
        return keyValue;
    }

}
