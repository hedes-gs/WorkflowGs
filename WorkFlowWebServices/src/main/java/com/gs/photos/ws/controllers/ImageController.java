package com.gs.photos.ws.controllers;

import java.beans.PropertyEditorSupport;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.reactive.WebFluxLinkBuilder;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photos.ws.repositories.IImageRepository;
import com.gs.photos.ws.services.KafkaConsumerService;
import com.gs.photos.ws.web.assembler.ImageAssembler;
import com.gs.photos.ws.web.assembler.MinMaxDateAssembler;
import com.gs.photos.ws.web.assembler.PageImageAssembler;
import com.workflow.model.dtos.ExchangedImageDto;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.MinMaxDatesDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/gs")
public class ImageController {

    protected static Logger        LOGGER = LoggerFactory.getLogger(ImageController.class);

    @Autowired
    protected KafkaConsumerService kafkaConsumerService;

    @Autowired
    protected IImageRepository     repository;

    @Autowired
    protected ImageAssembler       imageAssembler;

    @Autowired
    protected MinMaxDateAssembler  minMaxDateAssembler;

    @Autowired
    protected PageImageAssembler   pagedAssembler;

    public ImageController() {}

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(ImageKeyDto.class, new ImageKeyDtoTypeEditor());
        binder.registerCustomEditor(Sort.class, new SortTypeEditor());
        binder.registerCustomEditor(OffsetDateTime.class, new PropertyEditorSupport() {
            @Override
            public void setAsText(String text) throws java.lang.IllegalArgumentException {
                this.setValue(DateTimeHelper.toOffsetDateTime(text, DateTimeHelper.SPRING_VALUE_DATE_TIME_FORMATTER));
            }
        });
    }

    @GetMapping("/images/count/all")
    public @ResponseBody ResponseEntity<Long> countAll() throws IOException {
        return ResponseEntity.ok(this.repository.countAll());
    }

    @PostMapping("/images/checkout/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ExchangedImageDto>> checkout(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        Optional<ImageDto> img = this.repository.findById(salt, creationDate, id, version);
        img.ifPresent((i) -> this.kafkaConsumerService.checkout(i));
        return this.imageAssembler.toModel(
            img.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow(),
            null);
    }

    @PostMapping("/images/delete/{salt}/{id}/{creationDate}")
    public Mono<EntityModel<ExchangedImageDto>> delete(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate
    ) throws IOException {
        Optional<ExchangedImageDto> img = this.repository.delete(salt, creationDate, id)
            .map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build());
        return this.imageAssembler.toModel(img.orElseThrow(), null);
    }

    @GetMapping("/images/count/all/{intervallType}")
    public @ResponseBody ResponseEntity<Long> countAllBy(@PathVariable String intervallType) throws IOException {
        return ResponseEntity.ok(this.repository.countAllOf(intervallType));
    }

    @GetMapping("/images/dates/limits")
    public @ResponseBody ResponseEntity<MinMaxDatesDto> getLimits() {
        return ResponseEntity.ok(this.repository.getDatesLimit());
    }

    @GetMapping(path = "/images/dates/limits/allyears", produces = "application/stream+json")
    public Flux<EntityModel<MinMaxDatesDto>> getAllYears() throws IOException {
        MinMaxDatesDto minMax = this.repository.getDatesLimit();
        Flux<MinMaxDatesDto> result = this.repository.getListOfYearsBetween(
            minMax.getMinDate()
                .with(TemporalAdjusters.firstDayOfYear())
                .withHour(0)
                .withMinute(0)
                .withSecond(0),
            minMax.getMaxDate()
                .with(TemporalAdjusters.lastDayOfYear())
                .withHour(23)
                .withMinute(59)
                .withSecond(59));
        return this.minMaxDateAssembler.toFlux(result, null);
    }

    @GetMapping(path = "/images/odt/dates/limits/{intervalType}/{firstDate}/{lastDate}", produces = "application/stream+json")
    public Flux<EntityModel<MinMaxDatesDto>> getLimits(
        @PathVariable String intervalType,
        @PathVariable OffsetDateTime firstDate,
        @PathVariable OffsetDateTime lastDate
    ) throws IOException {
        Flux<MinMaxDatesDto> result = Flux.empty();
        switch (intervalType) {
            case "year":
                result = this.repository.getListOfYearsBetween(
                    firstDate.with(TemporalAdjusters.firstDayOfYear())
                        .withHour(0)
                        .withMinute(0)
                        .withSecond(0),
                    lastDate.with(TemporalAdjusters.lastDayOfYear())
                        .withHour(23)
                        .withMinute(59)
                        .withSecond(59));
                break;
            case "month":
                result = this.repository.getListOfMonthsBetween(
                    firstDate.with(TemporalAdjusters.firstDayOfMonth())
                        .withHour(0)
                        .withMinute(0)
                        .withSecond(0),
                    lastDate.with(TemporalAdjusters.lastDayOfMonth())
                        .withHour(23)
                        .withMinute(59)
                        .withSecond(59));
                break;
            case "day":
                result = this.repository.getListOfDaysBetween(
                    firstDate.withHour(0)
                        .withMinute(0)
                        .withSecond(0),
                    lastDate.withHour(23)
                        .withMinute(59)
                        .withSecond(59));
                break;
            case "hour":
                result = this.repository.getListOfHoursBetween(
                    firstDate.withMinute(0)
                        .withSecond(0),
                    lastDate.withMinute(59)
                        .withSecond(59));
                break;
            case "minute":
                result = this.repository.getListOfMinutesBetween(firstDate.withSecond(0), lastDate.withSecond(59));
                break;
            case "second":
                result = this.repository.getListOfSecondsBetween(firstDate, lastDate);
                break;
        }

        return this.minMaxDateAssembler.toFlux(result, null);
    }

    @GetMapping("/image/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ExchangedImageDto>> getImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            this.repository.findById(salt, creationDate, id, version)
                .map(
                    (x) -> ExchangedImageDto.builder()
                        .withImage(x)
                        .build())
                .get());
    }

    @GetMapping("/image/next/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ExchangedImageDto>> getNextImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            this.repository.getNextImageById(salt, creationDate, id, version)
                .map(
                    (x) -> ExchangedImageDto.builder()
                        .withImage(x)
                        .build())
                .get());
    }

    @GetMapping("/image/prev/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ExchangedImageDto>> getPreviousImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            ExchangedImageDto.builder()
                .withImage(
                    this.repository.getPreviousImageById(salt, creationDate, id, version)
                        .get())
                .build());
    }

    @GetMapping(path = "/images", produces = "application/stream+json")
    public Flux<EntityModel<ExchangedImageDto>> getLastImages(@PageableDefault(size = 100, page = 1) Pageable p)
        throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        return this.imageAssembler.toFlux(
            this.repository.findLastImages(p),
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getLastImages(p))
                .withRel("page")
                .toMono(),
            null);
    }

    @GetMapping(path = "/images/keyword/{keyword}", produces = "application/stream+json")
    public Flux<EntityModel<ExchangedImageDto>> getImagesByKeyword(
        @PageableDefault(size = 100, page = 1) Pageable p,
        @PathVariable String keyword
    ) throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        return this.imageAssembler.toFlux(
            this.repository.findImagesByKeyword(p, keyword),
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getImagesByKeyword(p, keyword))
                .withRel("page")
                .toMono(),
            null);
    }

    @GetMapping(path = "/images/person/{person}", produces = "application/stream+json")
    public Flux<EntityModel<ExchangedImageDto>> getImagesByPerson(
        @PageableDefault(size = 100, page = 1) Pageable p,
        @PathVariable String person
    ) throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        return this.imageAssembler.toFlux(
            this.repository.findImagesByPerson(p, person),
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getImagesByPerson(p, person))
                .withRel("page")
                .toMono(),
            null);
    }

    @GetMapping(path = "/images/albums/{album}", produces = "application/stream+json")
    public Flux<EntityModel<ExchangedImageDto>> getImagesByAlbum(
        @PageableDefault(size = 100, page = 1) Pageable p,
        @PathVariable String album
    ) throws IOException {
        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        return this.imageAssembler.toFlux(
            this.repository.findImagesByAlbum(p, album),
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getImagesByAlbum(p, album))
                .withRel("page")
                .toMono(),
            null);
    }

    @GetMapping(path = "/images/odt/{intervalType}/{startTime}/{stopDate}", produces = "application/stream+json")
    public Flux<EntityModel<ExchangedImageDto>> get(
        @PageableDefault(size = 100, page = 1) Pageable p,
        @PathVariable String intervalType,
        @PathVariable OffsetDateTime startTime,
        @PathVariable OffsetDateTime stopDate
    ) throws IOException {

        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }
        long totalNbOfElements = 0;
        Flux<ImageDto> retValue = Flux.empty();
        switch (intervalType) {
            case "year":
                totalNbOfElements = this.repository.countByYear(startTime, stopDate);
                retValue = this.repository.getThumbNailsByYear(startTime, stopDate, p, (short) 1);
                break;
            case "month":
                totalNbOfElements = this.repository.countByMonth(startTime, stopDate);
                retValue = this.repository.getThumbNailsByMonth(startTime, stopDate, p, (short) 1);
                break;
            case "day":
                totalNbOfElements = this.repository.countByDay(startTime, stopDate);
                retValue = this.repository.getThumbNailsByDay(startTime, stopDate, p, (short) 1);
                break;
            case "hour":
                totalNbOfElements = this.repository.countByHour(startTime, stopDate);
                retValue = this.repository.getThumbNailsByHour(startTime, stopDate, p, (short) 1);
                break;
            case "minute":
                totalNbOfElements = this.repository.countByMinute(startTime, stopDate);
                retValue = this.repository.getThumbNailsByMinute(startTime, stopDate, p, (short) 1);
                break;
            case "second":
                totalNbOfElements = this.repository.countBySecond(startTime, stopDate);
                retValue = this.repository.getThumbNailsBySecond(startTime, stopDate, p, (short) 1);
                break;
        }
        return this.imageAssembler.toFlux(
            retValue,
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .get(p, intervalType, startTime, stopDate))
                .withRel("page")
                .toMono(),
            (ServerWebExchange) null,
            startTime.toLocalDateTime(),
            stopDate.toLocalDateTime(),
            totalNbOfElements);
    }

    @GetMapping(value = "/image-jpeg/{salt}/{id}/{creationDate}/{version}", produces = MediaType.IMAGE_JPEG_VALUE)
    public @ResponseBody ResponseEntity<byte[]> getImageWithMediaType(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        ImageController.LOGGER.info("getImageWithMediaType : id is {}, version is {} ", id, version);

        CacheControl cacheControl = CacheControl.maxAge(1, TimeUnit.DAYS)
            .noTransform()
            .mustRevalidate();

        byte[] img = this.repository.getJpegImage(salt, creationDate, id, version);
        try {
            return ResponseEntity.ok()
                .cacheControl(cacheControl)
                .body(IOUtils.toByteArray(new ByteArrayInputStream(img)));
        } catch (IOException e) {
            throw new IllegalAccessError();
        }

    }

    @PostMapping(path = "/update")
    public Mono<EntityModel<ExchangedImageDto>> updateRating(@RequestBody ImageDto imageDto) {
        Optional<ImageDto> retValue = this.repository.updateRating(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            imageDto.getRatings());
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/keywords/addToImage/{keyword}")
    public Mono<EntityModel<ExchangedImageDto>> addKeyword(
        @RequestBody ImageDto imageDto,
        @PathVariable String keyword
    ) {
        Optional<ImageDto> retValue = this.repository.addKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/keywords/deleteInImage/{keyword}")
    public Mono<EntityModel<ExchangedImageDto>> deleteKeyword(
        @RequestBody ImageDto imageDto,
        @PathVariable String keyword
    ) {
        Optional<ImageDto> retValue = this.repository.deleteKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/persons/addToImage/{person}")
    public Mono<EntityModel<ExchangedImageDto>> addPerson(@RequestBody ImageDto imageDto, @PathVariable String person) {
        Optional<ImageDto> retValue = this.repository.addPerson(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            person);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/persons/deleteInImage/{keyword}")
    public Mono<EntityModel<ExchangedImageDto>> deletePerson(
        @RequestBody ImageDto imageDto,
        @PathVariable String keyword
    ) {
        Optional<ImageDto> retValue = this.repository.deletePerson(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/albums/addToImage/{person}")
    public Mono<EntityModel<ExchangedImageDto>> addAlbum(@RequestBody ImageDto imageDto, @PathVariable String person) {
        Optional<ImageDto> retValue = this.repository.addAlbum(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            person);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostMapping(path = "/albums/deleteInImage/{keyword}")
    public Mono<EntityModel<ExchangedImageDto>> deleteAlbum(
        @RequestBody ImageDto imageDto,
        @PathVariable String keyword
    ) {
        Optional<ImageDto> retValue = this.repository.deleteAlbum(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toReactiveEntityModel(
            retValue.map(
                (x) -> ExchangedImageDto.builder()
                    .withImage(x)
                    .build())
                .orElseThrow());
    }

    @PostConstruct
    private void init() { ImageController.LOGGER.info("Init controller !"); }

}
