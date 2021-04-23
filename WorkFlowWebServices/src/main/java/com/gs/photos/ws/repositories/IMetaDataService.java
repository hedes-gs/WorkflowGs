package com.gs.photos.ws.repositories;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImagesOfMetadata;

import reactor.core.publisher.Flux;

public interface IMetaDataService<T extends HbaseImagesOfMetadata, U extends HbaseData> {

    public Flux<T> getPage(U metadata, int pageNumber, int pageSize);

}
