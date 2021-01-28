package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.hateoas.EntityModel;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Repository;

import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.ws.repositories.IHbaseImageThumbnailDAO;
import com.gs.photos.ws.repositories.IImageRepository;
import com.gs.photos.ws.services.IHFileServices;
import com.gs.photos.ws.web.assembler.ImageAssembler;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.MinMaxDatesDto;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

import reactor.core.publisher.Flux;

@Repository
public class ImageRepositoryImpl implements IImageRepository {

    private static Logger               LOGGER = LoggerFactory.getLogger(ImageRepositoryImpl.class);

    @Autowired
    protected ImageAssembler            imageAssembler;

    @Autowired
    protected IHbaseImageThumbnailDAO   hbaseImageThumbnailDAO;

    @Autowired
    protected IHbaseStatsDAO            ihbaseStatsDAO;

    @Autowired
    protected IHbaseImagesOfKeywordsDAO ihbaseImagesOfKeywordsDAO;

    @Autowired
    protected IHbaseImagesOfPersonsDAO  ihbaseImagesOfPersonsDAO;

    @Autowired
    protected IHFileServices            ihFileServices;

    // @Autowired
    protected SimpMessagingTemplate     template;

    @Override
    public long countAll() throws IOException {
        MinMaxDatesDto minMaxDatesDto = this.getDatesLimit();
        return this.ihbaseStatsDAO
            .countImages(minMaxDatesDto.getMinDate(), minMaxDatesDto.getMaxDate(), KeyEnumType.YEAR);
    }

    @Override
    public long countAllOf(String intervalType) throws IOException {
        return this.ihbaseStatsDAO.countImages(KeyEnumType.valueOf(intervalType.toUpperCase()));
    }

    @Override
    public Optional<ImageDto> findById(short salt, OffsetDateTime creationDate, String id, int version) {
        return Optional.of(this.hbaseImageThumbnailDAO.findById(salt, creationDate, id, version));
    }

    @Override
    public Optional<ImageDto> getNextImageById(short salt, OffsetDateTime creationDate, String id, int version) {
        Optional<ImageDto> nextImageById = this.hbaseImageThumbnailDAO
            .getNextImageById(salt, creationDate, id, version);
        return nextImageById;
    }

    @Override
    public Optional<ImageDto> getPreviousImageById(short salt, OffsetDateTime creationDate, String id, int version) {
        Optional<ImageDto> previousImageById = this.hbaseImageThumbnailDAO
            .getPreviousImageById(salt, creationDate, id, version);
        return previousImageById;
    }

    @Override
    public Flux<ImageDto> getThumbNailsByYear(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.YEAR, versions);
        return images;
    }

    @Override
    public Flux<ImageDto> getThumbNailsByMonth(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.MONTH, versions);
        return images;
    }

    @Override
    public Flux<ImageDto> getThumbNailsByDay(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.DAY, versions);
        return images;
    }

    @Override
    public Flux<ImageDto> getThumbNailsByHour(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.HOUR, versions);
        return images;
    }

    @Override
    public Flux<ImageDto> getThumbNailsByMinute(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.MINUTE, versions);
        return images;
    }

    @Override
    public Flux<ImageDto> getThumbNailsBySecond(
        OffsetDateTime firstDate,
        OffsetDateTime lastDate,
        Pageable page,
        short... versions
    ) throws IOException {
        Flux<ImageDto> images = this.hbaseImageThumbnailDAO
            .getThumbNailsByDate(firstDate, lastDate, page, KeyEnumType.SECOND, versions);
        return images;
    }

    @Override
    public byte[] getJpegImage(short salt, OffsetDateTime creationDate, String id, int version) {
        return this.hbaseImageThumbnailDAO.findImageRawById(salt, creationDate, id, version);
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
    public Flux<ImageDto> findLastImages(Pageable page) throws IOException {
        return this.hbaseImageThumbnailDAO.findLastImages(page.getPageSize(), page.getPageNumber());
    }

    @Override
    public Flux<ImageDto> findImagesByKeyword(Pageable page, String keyword) throws IOException {
        Flux<ImageDto> retValue = this.hbaseImageThumbnailDAO
            .findLastImagesByKeyword(page.getPageSize(), page.getPageNumber(), keyword);
        return retValue;
    }

    @Override
    public Flux<ImageDto> findImagesByPerson(Pageable page, String person) throws IOException {
        Flux<ImageDto> retValue = this.hbaseImageThumbnailDAO
            .findLastImagesByPerson(page.getPageSize(), page.getPageNumber(), person);
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
    public Optional<ImageDto> updateRating(String id, OffsetDateTime creationDate, int version, long rating) {
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
    public Optional<ImageDto> addPerson(String imageId, OffsetDateTime creationDate, int version, String person) {
        return this.hbaseImageThumbnailDAO.addPerson(imageId, creationDate, version, person);
    }

    @Override
    public Optional<ImageDto> deletePerson(String imageId, OffsetDateTime creationDate, int version, String person) {
        return this.hbaseImageThumbnailDAO.deletePerson(imageId, creationDate, version, person);
    }

    @Override
    public void addAlbum(String id, OffsetDateTime creationDate, int version, String album) {
        this.hbaseImageThumbnailDAO.addAlbum(id, creationDate, version, album);
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

    @Override
    public void delete(short salt, OffsetDateTime creationDate, String id, int version) {
        final ImageDto imageToDelete = this.hbaseImageThumbnailDAO.findById(salt, creationDate, id, version);
        this.ihFileServices.delete(imageToDelete);
        this.hbaseImageThumbnailDAO.delete(creationDate, id, version);

    }

    int nbOfMessages = 0;

    @KafkaListener(topics = "${topic.topicEvent}", containerFactory = "kafkaListenerContainerFactoryForEvent")
    public void consumeFullyImageProcessed(@Payload(required = false) WfEvents message) {
        if (message != null) {
            message.getEvents()
                .stream()
                .filter(
                    (e) -> e.getStep()
                        .equals(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
                        && (e instanceof WfEventRecorded)
                        && (((WfEventRecorded) e).getRecordedEventType() == RecordedEventType.THUMB))
                .map((e) -> this.findImageByEvent(e))
                .filter((e) -> e != null)
                .map((e) -> this.hbaseImageThumbnailDAO.toImageDTO(e))
                .peek((e) -> ImageRepositoryImpl.LOGGER.info("[THUMBNAIL_DAO]Found image {}", e))
                .map((e) -> this.extracted(e))
                .forEach((e) -> this.processIncomingMessage(e));
            if (this.nbOfMessages++ > 25) {
                try {
                    TimeUnit.MILLISECONDS.sleep(5000);
                    this.nbOfMessages = 0;
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        } else {
            ImageRepositoryImpl.LOGGER.warn("Kafka : Receive message null !");
        }
    }

    protected void processIncomingMessage(EntityModel<ImageDto> e) {
        this.hbaseImageThumbnailDAO.invalidCache();
        this.template.convertAndSend("/topic/realtimeImportImages", e);
    }

    protected EntityModel<ImageDto> extracted(ImageDto e) { return this.imageAssembler.toEntityModel(e); }

    private HbaseImageThumbnail findImageByEvent(WfEvent e) {
        WfEventRecorded event = (WfEventRecorded) e;
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        HbaseImageThumbnail hbaseData = builder.withCreationDate(event.getImageCreationDate())
            .withImageId(e.getImgId())
            .build();
        try {
            return this.hbaseImageThumbnailDAO.get(hbaseData);
        } catch (IOException e1) {
            ImageRepositoryImpl.LOGGER.warn("Unexpected error", e1);
            throw new RuntimeException(e1);
        }
    }

}
