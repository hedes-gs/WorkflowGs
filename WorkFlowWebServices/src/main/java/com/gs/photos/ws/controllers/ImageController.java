package com.gs.photos.ws.controllers;

import java.beans.PropertyEditorSupport;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.List;
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

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photos.ws.repositories.IImageRepository;
import com.gs.photos.ws.services.KafkaConsumerService;
import com.gs.photos.ws.web.assembler.ImageAssembler;
import com.gs.photos.ws.web.assembler.PageImageAssembler;
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
    public Mono<EntityModel<ImageDto>> checkout(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        Optional<ImageDto> img = this.repository.findById(salt, creationDate, id, version);
        img.ifPresent((i) -> this.kafkaConsumerService.checkout(i));
        return this.imageAssembler.toModel(img.orElseThrow(), null);
    }

    @GetMapping("/images/delete/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ImageDto>> delete(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) throws IOException {
        Optional<ImageDto> img = this.repository.findById(salt, creationDate, id, version);
        img.ifPresent((i) -> this.kafkaConsumerService.checkout(i));
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

    @GetMapping("/image/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ImageDto>> getImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            this.repository.findById(salt, creationDate, id, version)
                .get());
    }

    @GetMapping("/image/next/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ImageDto>> getNextImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            this.repository.getNextImageById(salt, creationDate, id, version)
                .get());
    }

    @GetMapping("/image/prev/{salt}/{id}/{creationDate}/{version}")
    public Mono<EntityModel<ImageDto>> getPreviousImageById(
        @PathVariable short salt,
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        return this.imageAssembler.toReactiveEntityModel(
            this.repository.getPreviousImageById(salt, creationDate, id, version)
                .get());
    }

    @GetMapping(path = "/images", produces = "application/stream+json")
    public Flux<EntityModel<ImageDto>> getLastImages(@PageableDefault Pageable p) throws IOException {
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
    public Flux<EntityModel<ImageDto>> getImagesByKeyword(@PageableDefault Pageable p, @PathVariable String keyword)
        throws IOException {
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
    public Flux<EntityModel<ImageDto>> getImagesByPerson(@PageableDefault Pageable p, @PathVariable String person)
        throws IOException {
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

    @GetMapping(path = "/images/odt/{intervalType}/{startTime}/{stopDate}/{version}", produces = "application/stream+json")
    public Flux<EntityModel<ImageDto>> get(
        @PageableDefault Pageable p,
        @PathVariable String intervalType,
        @PathVariable OffsetDateTime startTime,
        @PathVariable OffsetDateTime stopDate,
        @PathVariable short version,
        PageImageAssembler pagedAssembler
    ) throws IOException {

        if (p.getPageSize() == 0) { throw new IllegalArgumentException("Illegal page size"); }

        Flux<ImageDto> retValue = Flux.empty();
        switch (intervalType) {
            case "year":
                retValue = this.repository.getThumbNailsByYear(startTime, stopDate, p, (short) 1);
                break;
            case "month":
                retValue = this.repository.getThumbNailsByMonth(startTime, stopDate, p, (short) 1);
                break;
            case "day":
                retValue = this.repository.getThumbNailsByDay(startTime, stopDate, p, (short) 1);
                break;
            case "hour":
                retValue = this.repository.getThumbNailsByHour(startTime, stopDate, p, (short) 1);
                break;
            case "minute":
                retValue = this.repository.getThumbNailsByMinute(startTime, stopDate, p, (short) 1);
                break;
            case "second":
                retValue = this.repository.getThumbNailsBySecond(startTime, stopDate, p, (short) 1);
                break;
        }
        return this.imageAssembler.toFlux(
            retValue,
            new GsPageImpl(Collections.emptyList(), p, this.repository.countAll()),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .get(p, intervalType, startTime, stopDate, version, pagedAssembler))
                .withRel("page")
                .toMono(),
            null);
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
    public Mono<EntityModel<ImageDto>> updateRating(@RequestBody ImageDto imageDto) {
        Optional<ImageDto> retValue = this.repository.updateRating(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            imageDto.getRatings());
        return this.imageAssembler.toModel(retValue.orElseThrow(), null);
    }

    @PostMapping(path = "/keywords/addToImage/{keyword}")
    public Mono<EntityModel<ImageDto>> addKeyword(@RequestBody ImageDto imageDto, @PathVariable String keyword) {
        Optional<ImageDto> retValue = this.repository.addKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toModel(retValue.orElseThrow(), null);
    }

    @PostMapping(path = "/keywords/deleteInImage/{keyword}")
    public Mono<EntityModel<ImageDto>> deleteKeyword(@RequestBody ImageDto imageDto, @PathVariable String keyword) {
        Optional<ImageDto> retValue = this.repository.deleteKeyword(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toModel(retValue.orElseThrow(), null);
    }

    @PostMapping(path = "/persons/addToImage/{person}")
    public Mono<EntityModel<ImageDto>> addPerson(@RequestBody ImageDto imageDto, @PathVariable String person) {
        Optional<ImageDto> retValue = this.repository.addPerson(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            person);
        return this.imageAssembler.toModel(retValue.orElseThrow(), null);
    }

    @PostMapping(path = "/persons/deleteInImage/{keyword}")
    public Mono<EntityModel<ImageDto>> deletePerson(@RequestBody ImageDto imageDto, @PathVariable String keyword) {
        Optional<ImageDto> retValue = this.repository.deletePerson(
            imageDto.getData()
                .getImageId(),
            imageDto.getData()
                .getCreationDate(),
            imageDto.getData()
                .getVersion(),
            keyword);
        return this.imageAssembler.toModel(retValue.orElseThrow(), null);
    }

    @PostConstruct
    private void init() { ImageController.LOGGER.info("Init controller !"); }

}
