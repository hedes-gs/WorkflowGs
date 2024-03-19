package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;

import com.workflow.model.HbaseImageThumbnail;

import reactor.core.publisher.Flux;

public interface IHbaseImagesOfMetadataDAO<T extends HbaseImageThumbnail, T1> {

    Flux<T> getPrevious(T1 meta, T metaData) throws IOException;

    public Flux<T> getNext(T1 meta, T metaData) throws IOException;

    public Flux<T> getAllImagesOfMetadata(T1 key, int first, int pageSize);

    void addMetaData(HbaseImageThumbnail hbi, T1 medataData);

    void deleteMetaData(HbaseImageThumbnail hbi, T1 metaData);

    void flush() throws IOException;

    public long countAll(T1 key) throws IOException, Throwable;
}
