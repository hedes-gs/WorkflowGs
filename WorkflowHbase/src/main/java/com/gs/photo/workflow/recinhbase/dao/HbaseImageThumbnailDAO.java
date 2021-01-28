package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImageThumbnailDAO;

@Component
public class HbaseImageThumbnailDAO extends AbstractHbaseImageThumbnailDAO implements IHbaseImageThumbnailDAO {

    @Override
    public void truncate() throws IOException { super.truncate(this.getHbaseDataInformation()); }

}
