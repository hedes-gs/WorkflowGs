package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfKeywordsDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfKeywordsDAO extends AbstractHbaseImagesOfKeywordsDAO implements IHbaseImagesOfKeyWordsDAO {

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