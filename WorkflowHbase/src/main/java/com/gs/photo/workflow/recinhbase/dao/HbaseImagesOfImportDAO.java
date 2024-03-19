package com.gs.photo.workflow.recinhbase.dao;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseImagesOfImportDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class HbaseImagesOfImportDAO extends AbstractHbaseImagesOfImportDAO implements IHbaseImagesOfImportDAO {

    @Override
    public void truncate() throws IOException {
        super.truncate(this.getHbaseDataInformation());
        ((IHbaseImportDAO) this.hbaseImportDAO).truncate();
    }

    @Override
    public List<HbaseImageThumbnail> getAllImagesOfMetadata(String album) {
        return super.getAllImagesOfMetadata(album, 1, 100000).collectList()
            .block();
    }

}
