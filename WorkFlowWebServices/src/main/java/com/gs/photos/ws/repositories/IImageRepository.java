package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.MinMaxDatesDto;

import reactor.core.publisher.Flux;

public interface IImageRepository {

    Optional<ImageDto> findById(short salt, OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> delete(short salt, OffsetDateTime creationDate, String id) throws IOException;

    Optional<ImageDto> getNextImageById(short salt, OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getPreviousImageById(short salt, OffsetDateTime creationDate, String id, int version);

    Flux<ImageDto> findLastImages(Pageable page) throws IOException;

    Flux<ImageDto> findImagesByKeyword(Pageable page, String keyword) throws IOException;

    Flux<ImageDto> findImagesByPerson(Pageable page, String person) throws IOException;

    Flux<ImageDto> findImagesByAlbum(Pageable page, String album) throws IOException;

    public byte[] getJpegImage(short salt, OffsetDateTime creationDate, String id, int version);

    public MinMaxDatesDto getDatesLimit();

    long countAll() throws IOException;

    long countByYear(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByMonth(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByDay(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByHour(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByMinute(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countBySecond(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    Optional<ImageDto> getNextImageOfRatingById(OffsetDateTime creationDate, String id, int version, int rating);

    Optional<ImageDto> getPreviousImageOfRatingById(OffsetDateTime creationDate, String id, int version, int rating);

    Optional<ImageDto> getNextImageOfKeywordById(OffsetDateTime creationDate, String id, int version, String keyword);

    Optional<ImageDto> getPreviousImageOfKeywordById(
        OffsetDateTime creationDate,
        String id,
        int version,
        String keyword
    );

    public Flux<ImageDto> getThumbNailsByYear(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<ImageDto> getThumbNailsByMonth(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<ImageDto> getThumbNailsByDay(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<ImageDto> getThumbNailsByHour(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<ImageDto> getThumbNailsByMinute(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<ImageDto> getThumbNailsBySecond(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Flux<MinMaxDatesDto> getListOfYearsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public Flux<MinMaxDatesDto> getListOfMonthsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public Flux<MinMaxDatesDto> getListOfDaysBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public Flux<MinMaxDatesDto> getListOfHoursBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public Flux<MinMaxDatesDto> getListOfMinutesBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public Flux<MinMaxDatesDto> getListOfSecondsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    long countAllOf(String intervalType) throws IOException;

    Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating);

    Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> addAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteKeyword(String imageId, OffsetDateTime creationDate, int version, String keyword);

    Optional<ImageDto> addPerson(String imageId, OffsetDateTime creationDate, int version, String person);

    Optional<ImageDto> deletePerson(String imageId, OffsetDateTime creationDate, int version, String keyword);

}
