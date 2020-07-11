package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractMetaDataDAO;
import com.workflow.model.HbaseAlbum;

@Component
public class HbaseAlbumDAO extends AbstractMetaDataDAO<HbaseAlbum, String> implements IHbaseAlbumDAO {

    @Override
    public long countAll() throws IOException, Throwable {
        return super.countWithCoprocessorJob(this.getHbaseDataInformation());
    }

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    protected byte[] createKey(String album) throws IOException {
        HbaseAlbum hbaseAlbum = HbaseAlbum.builder()
            .withAlbumName(album)
            .build();

        byte[] keyValue = new byte[this.getHbaseDataInformation()
            .getKeyLength()];
        this.getHbaseDataInformation()
            .buildKey(hbaseAlbum, keyValue);
        return keyValue;
    }

    @Override
    public void incrementNbOfImages(HbaseAlbum metaData) throws IOException {
        super.incrementNbOfImages(metaData.getAlbumName());
    }

    @Override
    public void decrementNbOfImages(HbaseAlbum metaData) throws IOException {
        super.decrementNbOfImages(metaData.getAlbumName());
    }

    @Override
    public long countAll(HbaseAlbum metaData) throws IOException, Throwable {
        return super.countAll(metaData.getAlbumName());
    }

}
