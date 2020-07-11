package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.controllers.GsPageImpl;
import com.gs.photos.repositories.IHbaseImageThumbnailDAO;
import com.gs.photos.repositories.IImageRepository;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.MinMaxDatesDto;

@Repository
public class ImageRepositoryImpl implements IImageRepository {

    @Autowired
    protected IHbaseImageThumbnailDAO   hbaseImageThumbnailDAO;

    @Autowired
    protected IHbaseStatsDAO            ihbaseStatsDAO;

    @Autowired
    protected IHbaseImagesOfKeywordsDAO ihbaseImagesOfKeywordsDAO;

    @Override
    public long countAll() throws IOException {
        MinMaxDatesDto minMaxDatesDto = this.getDatesLimit();
        return this.ihbaseStatsDAO
            .countImages(minMaxDatesDto.getMinDate(), minMaxDatesDto.getMaxDate(), KeyEnumType.SECOND);
    }

    @Override
    public long countAllOf(String intervalType) throws IOException {
        return this.ihbaseStatsDAO.countImages(KeyEnumType.valueOf(intervalType.toUpperCase()));
    }

    @Override
    public Optional<ImageDto> findById(OffsetDateTime creationDate, String id, int version) {
        return Optional.of(this.hbaseImageThumbnailDAO.findById(creationDate, id, version));
    }

    @Override
    public Optional<ImageDto> getNextImageById(OffsetDateTime creationDate, String id, int version) {
        Optional<ImageDto> nextImageById = this.hbaseImageThumbnailDAO.getNextImageById(creationDate, id, version);
        return nextImageById;
    }

    @Override
    public Optional<ImageDto> getPreviousImageById(OffsetDateTime creationDate, String id, int version) {
        Optional<ImageDto> previousImageById = this.hbaseImageThumbnailDAO
            .getPreviousImageById(creationDate, id, version);
        return previousImageById;
    }

    @Override
    public Page<ImageDto> getThumbNailsByYear(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page, versions);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.YEAR));

        return retValue;
    }

    @Override
    public Page<ImageDto> getThumbNailsByMonth(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page, versions);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.MONTH));

        return retValue;
    }

    @Override
    public Page<ImageDto> getThumbNailsByDay(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page, versions);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.DAY));

        return retValue;
    }

    @Override
    public Page<ImageDto> getThumbNailsByHour(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page, versions);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.HOUR));

        return retValue;
    }

    @Override
    public Page<ImageDto> getThumbNailsByMinute(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page, versions);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.MINUTE));

        return retValue;
    }

    @Override
    public Page<ImageDto> getThumbNailsBySecond(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.getThumbNailsByDate(firstDate, lastDate, page);
        GsPageImpl retValue = new GsPageImpl(images,
            page,
            this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.SECOND));

        return retValue;
    }

    @Override
    public byte[] getJpegImage(OffsetDateTime creationDate, String id, int version) {
        return this.hbaseImageThumbnailDAO.findImageRawById(creationDate, id, version);
    }

    @Override
    public long countByYear(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.YEAR);
    }

    @Override
    public long countByMonth(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.MONTH);
    }

    @Override
    public long countByDay(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.DAY);
    }

    @Override
    public long countByHour(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.HOUR);
    }

    @Override
    public long countByMinute(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.MINUTE);
    }

    @Override
    public long countBySecond(OffsetDateTime firstDate, OffsetDateTime lastDate) throws IOException {
        return this.ihbaseStatsDAO.countImages(firstDate, lastDate, KeyEnumType.SECOND);
    }

    @Override
    public Page<ImageDto> findLastImages(Pageable page) throws IOException {
        List<ImageDto> images = this.hbaseImageThumbnailDAO.findLastImages(page.getPageSize(), page.getPageNumber());
        GsPageImpl retValue = new GsPageImpl(images, page, this.countAll());
        return retValue;
    }

    @Override
    public MinMaxDatesDto getDatesLimit() { return this.ihbaseStatsDAO.getMinMaxDates(); }

    @Override
    public List<MinMaxDatesDto> getListOfYearsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.YEAR);
    }

    @Override
    public List<MinMaxDatesDto> getListOfMonthsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.MONTH);
    }

    @Override
    public List<MinMaxDatesDto> getListOfDaysBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.DAY);
    }

    @Override
    public List<MinMaxDatesDto> getListOfHoursBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.HOUR);
    }

    @Override
    public List<MinMaxDatesDto> getListOfMinutesBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.MINUTE);
    }

    @Override
    public List<MinMaxDatesDto> getListOfSecondsBetween(OffsetDateTime startTime, OffsetDateTime stopDate)
        throws IOException {
        return this.ihbaseStatsDAO.getDatesBetween(startTime, stopDate, KeyEnumType.SECOND);
    }

    @Override
    public Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, int rating) {
        return this.hbaseImageThumbnailDAO.updateRating(id, creationDate, version, rating);
    }

    @Override
    public Optional<ImageDto> addKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        return this.hbaseImageThumbnailDAO.addKeyword(id, creationDate, version, keyword);
    }

    @Override
    public Optional<ImageDto> deleteKeyword(String id, OffsetDateTime creationDate, int version, String keyword) {
        return this.hbaseImageThumbnailDAO.deleteKeyword(id, creationDate, version, keyword);
    }

    @Override
    public void addAlbum(String id, OffsetDateTime creationDate, int version, String album) {
        this.hbaseImageThumbnailDAO.addAlbum(id, creationDate, version, album);
    }

    @Override
    public Page<ImageDto> getImagesByKeyword(String keyword) throws IOException { // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Page<ImageDto> getImagesByRating(String keyword) throws IOException { // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<ImageDto> getNextImageOfRatingById(
        OffsetDateTime creationDate,
        String id,
        int version,
        int rating
    ) {
        return null;
    }

    @Override
    public Optional<ImageDto> getPreviousImageOfRatingById(
        OffsetDateTime creationDate,
        String id,
        int version,
        int rating
    ) {
        return null;
    }

    @Override
    public Optional<ImageDto> getNextImageOfKeywordById(
        OffsetDateTime creationDate,
        String id,
        int version,
        String keyword
    ) {
        return null;
    }

    @Override
    public Optional<ImageDto> getPreviousImageOfKeywordById(
        OffsetDateTime creationDate,
        String id,
        int version,
        String keyword
    ) {
        return null;
    }

}
