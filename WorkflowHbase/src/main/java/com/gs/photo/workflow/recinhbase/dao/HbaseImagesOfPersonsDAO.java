package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfPersonsDAO;
import com.gs.photo.common.workflow.hbase.dao.IPersonsDAO;

@Component
public class HbaseImagesOfPersonsDAO extends AbstractHbaseImagesOfPersonsDAO implements IHbaseImagesOfPersonsDAO {
    public HbaseImagesOfPersonsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IPersonsDAO hbasePersonDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbasePersonDAO);
    }

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbasePersonsDAO) this.hbasePersonDAO).truncate();
    }
}