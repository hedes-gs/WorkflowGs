package com.gs.photos.repositories;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.workflow.model.dtos.ImageDto;

public interface IHbaseImageThumbnailDAO {

    ImageDto findById(OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getNextImageById(OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getPreviousImageById(OffsetDateTime creationDate, String id, int version);

    byte[] findImageRawById(OffsetDateTime creationDate, String id, int version);

    List<ImageDto> getThumbNailsByDate(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    );

    int count(OffsetDateTime firstDate, OffsetDateTime lastDate);

    int count() throws Throwable;

    List<ImageDto> findLastImages(int pageSize, int pageNumber);

    Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, int rating);

    Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    void addAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    void delete(OffsetDateTime creationDate, String id, int version);

}
