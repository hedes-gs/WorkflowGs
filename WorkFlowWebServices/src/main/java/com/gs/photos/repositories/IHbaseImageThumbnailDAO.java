package com.gs.photos.repositories;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.gs.photo.workflow.dao.IImageThumbnailDAO;
import com.workflow.model.dtos.ImageDto;

public interface IHbaseImageThumbnailDAO extends IImageThumbnailDAO {

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

    long count(OffsetDateTime firstDate, OffsetDateTime lastDate);

    long count() throws Throwable;

    List<ImageDto> findLastImages(int pageSize, int pageNumber);

    Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating);

    Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> addPerson(String id, OffsetDateTime creationDate, int version, String person);

    void addAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> deletePerson(String id, OffsetDateTime creationDate, int version, String keyword);

    void delete(OffsetDateTime creationDate, String id, int version);

    List<ImageDto> findLastImagesByKeyword(int pageSize, int pageNumber, String keyword);

    List<ImageDto> findLastImagesByPerson(int pageSize, int pageNumber, String person);

}
