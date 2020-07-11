package com.gs.photos.repositories.impl;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.workflow.model.HbaseImagesOfAlbum;
import com.workflow.model.dtos.ImageDto;

public interface IHbaseAlbumDAO {

    Optional<ImageDto> getNextImageById(HbaseImagesOfAlbum album, OffsetDateTime creationDate, String id, int version);

    Optional<ImageDto> getPreviousImageById(HbaseImagesOfAlbum album, OffsetDateTime creationDate, String id, int version);

    public List<ImageDto> getThumbNailsByPage(OffsetDateTime firstDate, OffsetDateTime lastDate, Pageable page);
}
