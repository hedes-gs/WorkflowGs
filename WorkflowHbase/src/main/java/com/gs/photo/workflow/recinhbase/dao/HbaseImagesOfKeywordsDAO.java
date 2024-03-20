package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfKeywordsDAO;
import com.gs.photo.common.workflow.hbase.dao.IKeywordsDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfKeywordsDAO extends AbstractHbaseImagesOfKeywordsDAO implements IHbaseImagesOfKeyWordsDAO {

    public HbaseImagesOfKeywordsDAO(
        Connection connection,
        String nameSpace,
        IImageThumbnailDAO hbaseImageThumbnailDAO,
        IKeywordsDAO hbaseKeywordsDAO
    ) {
        super(connection,
            nameSpace,
            hbaseImageThumbnailDAO,
            hbaseKeywordsDAO);

    }

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseKeywordsDAO) this.hbaseKeywordsDAO).truncate();
    }

    @Override
    public List<HbaseImageThumbnail> getAllImagesOfMetadata(String album) { // TODO Auto-generated method stub
        return super.getAllImagesOfMetadata(album, 1, 100000).collectList()
            .block();
    }

}