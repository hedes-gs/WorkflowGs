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

    void addMetaData(HbaseImageThumbnail hbi, T1 medataData);

    void deleteMetaData(HbaseImageThumbnail hbi, T1 metaData);

    void flush() throws IOException;

    public long countAll() throws IOException, Throwable;

    public long countAll(T metaData) throws IOException, Throwable;

    public long countAll(T1 key) throws IOException, Throwable;
}
