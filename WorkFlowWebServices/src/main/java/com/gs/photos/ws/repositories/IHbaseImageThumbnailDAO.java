package com.gs.photos.ws.repositories;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Scan;
import org.springframework.data.domain.Pageable;

import com.gs.photo.common.workflow.dao.IImageThumbnailDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photo.common.workflow.hbase.dao.PageDescription;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;
import com.workflow.model.dtos.ImageVersionDto;

import reactor.core.publisher.Flux;

public interface IHbaseImageThumbnailDAO extends IImageThumbnailDAO {

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

    void delete(OffsetDateTime creationDate, String id) throws IOException;

    ImageDto toImageDTO(HbaseImageThumbnail e);

    void invalidCache();

    PageDescription<ImageKeyDto> loadPageInTablePage(long pageNumberInTablePage, int pageSize);

    Flux<HbaseImageThumbnail> getNextThumbNailsOf(HbaseImageThumbnail initialKey, boolean includeRow);

    Flux<HbaseImageThumbnail> getPreviousThumbNailsOf(HbaseImageThumbnail initialKey, boolean incluseRow);

    Flux<ImageDto> getSimpleList(final Scan scan);

    Flux<ImageKeyDto> getImageKeyDtoList(final Scan scan);

    ImageDto getImageDto(ImageKeyDto imageKeyDto, OffsetDateTime creationDate, String id, int version);

    Optional<ImageVersionDto> getImageVersionDto(OffsetDateTime creationDate, String id, int version);

    void delete(HbaseImageThumbnail hbaseData) throws IOException;

    ImageDto getImageDto(ImageKeyDto imageKeyDto);

}
