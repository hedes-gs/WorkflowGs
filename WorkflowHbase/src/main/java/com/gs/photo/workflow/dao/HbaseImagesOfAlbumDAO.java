package com.gs.photo.workflow.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseImagesOfAlbumDAO;

@Component
public class HbaseImagesOfAlbumDAO extends AbstractHbaseImagesOfAlbumDAO implements IHbaseImagesOfAlbumDAO {
    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseAlbumDAO) this.hbaseAlbumDAO).truncate();
    }
}
