package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseAlbumDAO;
import com.workflow.model.HbaseAlbum;

@Component
public class HbaseAlbumDAO extends AbstractHbaseAlbumDAO implements IHbaseAlbumDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

    @Override
    public void delete(HbaseAlbum hbaseData, String family, String column) { // TODO Auto-generated method stub
    }

}
