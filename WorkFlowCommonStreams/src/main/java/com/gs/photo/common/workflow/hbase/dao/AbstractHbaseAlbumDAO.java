package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

import com.workflow.model.HbaseAlbum;

public abstract class AbstractHbaseAlbumDAO extends AbstractMetaDataDAO<HbaseAlbum, String> implements IAlbumDAO {

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public long countAll(HbaseAlbum metaData) throws IOException, Throwable {
        return super.countAll(metaData.getAlbumName());
    }

    @Override
    protected byte[] createKey(String album) throws IOException {
        HbaseAlbum hbaseAlbum = HbaseAlbum.builder()
            .withAlbumName(album)
            .build();
        byte[] keyValue = this.getHbaseDataInformation()
            .buildKey(hbaseAlbum);
        return keyValue;
    }

    public AbstractHbaseAlbumDAO(
        Connection connection,
        String nameSpace
    ) { super(connection,
        nameSpace); }

}
