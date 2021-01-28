package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseAlbumDAO;

@Component
public class HbaseAlbumDAO extends AbstractHbaseAlbumDAO implements IHbaseAlbumDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

}
