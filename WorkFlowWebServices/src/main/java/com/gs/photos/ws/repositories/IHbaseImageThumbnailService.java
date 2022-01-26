package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.dtos.ImageDto;

import reactor.core.publisher.Flux;

public interface IHbaseImageThumbnailService {

    ImageDto findById(short salt, OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getNextImageById(short salt, OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getPreviousImageById(short salt, OffsetDateTime creationDate, String id, int version);

    byte[] findImageRawById(short salt, OffsetDateTime creationDate, String id, int version);

    Flux<ImageDto> getThumbNailsByDate(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        KeyEnumType keyType,
        short... versions
    );

    long count(OffsetDateTime firstDate, OffsetDateTime lastDate);

    long count() throws Throwable;

    Flux<ImageDto> findLastImages(int pageSize, int pageNumber);

    Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating);

    Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> addPerson(String id, OffsetDateTime creationDate, int version, String person);

    Optional<ImageDto> addAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> deletePerson(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> delete(OffsetDateTime creationDate, String id) throws IOException;

    Flux<ImageDto> findLastImagesByKeyword(int pageSize, int pageNumber, String keyword);

    Flux<ImageDto> findLastImagesByPerson(int pageSize, int pageNumber, String person);

    Flux<ImageDto> findImagesByAlbum(int pageSize, int pageNumber, String album);

    ImageDto toImageDTO(HbaseImageThumbnail e);

    void invalidCache();

}
