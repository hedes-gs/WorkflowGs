package com.gs.photos.controllers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.PagedModel;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.gs.photos.repositories.IImageRepository;
import com.gs.photos.services.KafkaConsumerService;
import com.gs.photos.web.assembler.ImageAssembler;
import com.gs.photos.web.assembler.PageImageAssembler;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.MinMaxDatesDto;

@RestController
@RequestMapping("/api/gs")
@CrossOrigin(origins = "*")
public class ImageController {

    protected static Logger        LOGGER = LoggerFactory.getLogger(ImageController.class);

    @Autowired
    protected KafkaConsumerService kafkaConsumerService;

    @Autowired
    protected IImageRepository     repository;

    @Autowired
    protected ImageAssembler       imageAssembler;

    public ImageController() {}

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(ImageKeyDto.class, new ImageKeyDtoTypeEditor());
        binder.registerCustomEditor(Sort.class, new SortTypeEditor());
    }

    @GetMapping("/images/count/all")
    public @ResponseBody ResponseEntity<Long> countAll() throws IOException {
        return ResponseEntity.ok(this.repository.countAll());
    }

    @GetMapping("/images/checkout/{id}/{creationDate}/{version}")
    public @ResponseBody ResponseEntity<?> checkout(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) throws IOException {
        Optional<ImageDto> img = this.repository.findById(creationDate, id, version);
        img.ifPresent((i) -> this.kafkaConsumerService.checkout(i));
        return ResponseEntity.ok(this.imageAssembler.toModel(img.orElseThrow()));
    }

    @GetMapping("/images/delete/{id}/{creationDate}/{version}")
    public @ResponseBody ResponseEntity<?> delete(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) throws IOException {
        Optional<ImageDto> img = this.repository.findById(creationDate, id, version);
        img.ifPresent((i) -> this.kafkaConsumerService.checkout(i));
        return ResponseEntity.ok(this.imageAssembler.toModel(img.orElseThrow()));
    }

    @GetMapping("/images/count/all/{intervallType}")
    public @ResponseBody ResponseEntity<Long> countAllBy(@PathVariable String intervallType) throws IOException {
        return ResponseEntity.ok(this.repository.countAllOf(intervallType));
    }

    @GetMapping("/images/dates/limits")
    public @ResponseBody ResponseEntity<MinMaxDatesDto> getLimits() {
        return ResponseEntity.ok(this.repository.getDatesLimit());
    }

    @GetMapping("/images/odt/dates/limits/{intervalType}/{firstDate}/{lastDate}")
    public @ResponseBody ResponseEntity<List<MinMaxDatesDto>> getLimits(
        @PathVariable String intervalType,
        @PathVariable OffsetDateTime firstDate,
        @PathVariable OffsetDateTime lastDate
    ) throws IOException {
        Optional<List<MinMaxDatesDto>> entity = Optional.empty();
        switch (intervalType) {
            case "year":
                entity = Optional.of(
                    this.repository.getListOfYearsBetween(
                        firstDate.with(TemporalAdjusters.firstDayOfYear())
                            .withHour(0)
                            .withMinute(0)
                            .withSecond(0),
                        lastDate.with(TemporalAdjusters.lastDayOfYear())
                            .withHour(23)
                            .withMinute(59)
                            .withSecond(59)));
                break;
            case "month":
                entity = Optional.of(
                    this.repository.getListOfMonthsBetween(
                        firstDate.with(TemporalAdjusters.firstDayOfMonth())
                            .withHour(0)
                            .withMinute(0)
                            .withSecond(0),
                        lastDate.with(TemporalAdjusters.lastDayOfMonth())
                            .withHour(23)
                            .withMinute(59)
                            .withSecond(59)));
                break;
            case "day":
                entity = Optional.of(
                    this.repository.getListOfDaysBetween(
                        firstDate.withHour(0)
                            .withMinute(0)
                            .withSecond(0),
                        lastDate.withHour(23)
                            .withMinute(59)
                            .withSecond(59)));
                break;
            case "hour":
                entity = Optional.of(
                    this.repository.getListOfHoursBetween(
                        firstDate.withMinute(0)
                            .withSecond(0),
                        lastDate.withMinute(59)
                            .withSecond(59)));
                break;
            case "minute":
                entity = Optional
                    .of(this.repository.getListOfMinutesBetween(firstDate.withSecond(0), lastDate.withSecond(59)));
                break;
            case "second":
                entity = Optional.of(this.repository.getListOfSecondsBetween(firstDate, lastDate));
                break;
        }

        return ResponseEntity.ok(entity.orElseThrow());
    }

    @GetMapping("/image/{id}/{creationDate}/{version}")
    public @ResponseBody ResponseEntity<?> getImageById(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return ResponseEntity.ok(
            this.imageAssembler.toModel(
                this.repository.findById(creationDate, id, version)
                    .get()));
    }

    @GetMapping("/image/{id}/{creationDate}/{version}/next")
    public @ResponseBody ResponseEntity<?> getNextImageById(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return ResponseEntity.ok(
            this.imageAssembler.toModel(
                this.repository.getNextImageById(creationDate, id, version)
                    .get()));
    }

    @GetMapping("/image/{id}/{creationDate}/{version}/prev")
    public @ResponseBody ResponseEntity<?> getPreviousImageById(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return ResponseEntity.ok(
            this.imageAssembler.toModel(
                this.repository.getPreviousImageById(creationDate, id, version)
                    .get()));
    }

    @GetMapping("/images")
    public @ResponseBody PagedModel<EntityModel<ImageDto>> getLastImages(
        @PageableDefault GsPageRequest p,
        PageImageAssembler pagedAssembler
    ) throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        Optional<Page<ImageDto>> entity = Optional.of(this.repository.findLastImages(p));
        return pagedAssembler.toModel(entity.orElseThrow(), this.imageAssembler);
    }

    @GetMapping("/images/keyword/{keyword}")
    public @ResponseBody PagedModel<EntityModel<ImageDto>> getImagesByKeyword(
        @PageableDefault GsPageRequest p,
        PageImageAssembler pagedAssembler,
        @PathVariable String keyword
    ) throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        Optional<Page<ImageDto>> entity = Optional.of(this.repository.findLastImages(p));
        return pagedAssembler.toModel(entity.orElseThrow(), this.imageAssembler);
    }

    @GetMapping("/images/odt/{intervalType}/{startTime}/{stopDate}/{version}")
    public PagedModel<EntityModel<ImageDto>> get(
        @PageableDefault GsPageRequest p,
        @PathVariable String intervalType,
        @PathVariable OffsetDateTime startTime,
        @PathVariable OffsetDateTime stopDate,
        @PathVariable short version,
        PageImageAssembler pagedAssembler
    ) throws IOException {

        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }

        Optional<Page<ImageDto>> entity = Optional.empty();
        switch (intervalType) {
            case "year":
                entity = Optional.of(this.repository.getThumbNailsByYear(startTime, stopDate, p, (short) 1));
                break;
            case "month":
                entity = Optional.of(this.repository.getThumbNailsByMonth(startTime, stopDate, p, (short) 1));
                break;
            case "day":
                entity = Optional.of(this.repository.getThumbNailsByDay(startTime, stopDate, p, (short) 1));
                break;
            case "hour":
                entity = Optional.of(this.repository.getThumbNailsByHour(startTime, stopDate, p, (short) 1));
                break;
            case "minute":
                entity = Optional.of(this.repository.getThumbNailsByMinute(startTime, stopDate, p, (short) 1));
                break;
            case "second":
                entity = Optional.of(this.repository.getThumbNailsBySecond(startTime, stopDate, p, (short) 1));
                break;
        }

        return pagedAssembler.toModel(entity.orElseThrow(), this.imageAssembler);
    }

    @GetMapping(value = "/image-jpeg/{id}/{creationDate}/{version}", produces = MediaType.IMAGE_JPEG_VALUE)
    public @ResponseBody ResponseEntity<byte[]> getImageWithMediaType(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        ImageController.LOGGER.info("getImageWithMediaType : id is {}, version is {} ", id, version);

        CacheControl cacheControl = CacheControl.maxAge(1, TimeUnit.DAYS)
            .noTransform()
            .mustRevalidate();

        byte[] img = this.repository.getJpegImage(creationDate, id, version);
        try {
            return ResponseEntity.ok()
                .cacheControl(cacheControl)
                .body(IOUtils.toByteArray(new ByteArrayInputStream(img)));
        } catch (IOException e) {
            throw new IllegalAccessError();
        }

    }

    @PostMapping(path = "/update")
    public ResponseEntity<?> updateRating(@RequestBody ImageDto imageDto) {
        Optional<ImageDto> retValue = this.repository.updateRating(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            imageDto.getRatings());
        return ResponseEntity.ok(this.imageAssembler.toModel(retValue.orElseThrow()));
    }

    @PostMapping(path = "/keywords/addToImage/{keyword}")
    public ResponseEntity<?> addKeyword(@RequestBody ImageDto imageDto, @PathVariable String keyword) {
        Optional<ImageDto> retValue = this.repository.addKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return ResponseEntity.ok(this.imageAssembler.toModel(retValue.orElseThrow()));
    }

    @PostMapping(path = "/keywords/deleteInImage/{keyword}")
    public ResponseEntity<?> deleteKeyword(@RequestBody ImageDto imageDto, @PathVariable String keyword) {
        Optional<ImageDto> retValue = this.repository.deleteKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return ResponseEntity.ok(this.imageAssembler.toModel(retValue.orElseThrow()));
    }

}
