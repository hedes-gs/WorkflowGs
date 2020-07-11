package com.gs.photo.workflow.hbase.dao;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageThumbnail;

public interface IHbaseImagesOfMetadataDAO<T extends HbaseData, T1> {

    Optional<T> getPrevious(T metaData) throws IOException;

    public Optional<T> getNext(T metaData) throws IOException;

    public List<T> getAllImagesOfMetadata(T1 key);

    public List<T> getAllImagesOfMetadata(T1 key, int first, int pageSize);

    void updateMetadata(HbaseImageThumbnail hbi, HbaseImageThumbnail previous) throws IOException;

    void updateMetadata(HbaseImageThumbnail hbi, T1 medataData);
}
