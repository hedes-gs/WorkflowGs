package com.gs.photos.repositories;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.MinMaxDatesDto;

public interface IImageRepository {

    Optional<ImageDto> findById(OffsetDateTime creationDate, String id, int version);

    void delete(OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getNextImageById(OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getPreviousImageById(OffsetDateTime creationDate, String id, int version);

    Page<ImageDto> findLastImages(Pageable page) throws IOException;

    public byte[] getJpegImage(OffsetDateTime creationDate, String id, int version);

    public MinMaxDatesDto getDatesLimit();

    long countAll() throws IOException;

    long countByYear(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByMonth(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByDay(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByHour(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countByMinute(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    long countBySecond(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException;

    public Page<ImageDto> getImagesByKeyword(String keyword) throws IOException;

    public Page<ImageDto> getImagesByRating(String keyword) throws IOException;

    Optional<ImageDto> getNextImageOfRatingById(OffsetDateTime creationDate, String id, int version, int rating);

    Optional<ImageDto> getPreviousImageOfRatingById(OffsetDateTime creationDate, String id, int version, int rating);

    Optional<ImageDto> getNextImageOfKeywordById(OffsetDateTime creationDate, String id, int version, String keyword);

    Optional<ImageDto> getPreviousImageOfKeywordById(
        OffsetDateTime creationDate,
        String id,
        int version,
        String keyword
    );

    public Page<ImageDto> getThumbNailsByYear(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Page<ImageDto> getThumbNailsByMonth(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Page<ImageDto> getThumbNailsByDay(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Page<ImageDto> getThumbNailsByHour(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Page<ImageDto> getThumbNailsByMinute(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public Page<ImageDto> getThumbNailsBySecond(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException;

    public List<MinMaxDatesDto> getListOfYearsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public List<MinMaxDatesDto> getListOfMonthsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public List<MinMaxDatesDto> getListOfDaysBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public List<MinMaxDatesDto> getListOfHoursBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public List<MinMaxDatesDto> getListOfMinutesBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    public List<MinMaxDatesDto> getListOfSecondsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException;

    long countAllOf(String intervalType) throws IOException;

    Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, int rating);

    Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword);

    void addAlbum(String id, OffsetDateTime creationDate, int version, String album);

    Optional<ImageDto> deleteKeyword(String imageId, OffsetDateTime creationDate, int version, String keyword);

}
