package com.gs.photos.ws.repositories.impl;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;

import com.gs.photo.common.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseImagesOfAlbum;
import com.workflow.model.dtos.ImageDto;

public class HbaseAlbumDAO extends GenericDAO<HbaseImagesOfAlbum> implements IHbaseAlbumDAO {

    @Override
    public Optional<ImageDto> getNextImageById(
        HbaseImagesOfAlbum album,
        OffsetDateTime creationDate,
        String id,
        int version
    ) {
        return Optional.empty();
    }

    @Override
    public Optional<ImageDto> getPreviousImageById(
        HbaseImagesOfAlbum album,
        OffsetDateTime creationDate,
        String id,
        int version
    ) {
        return Optional.empty();
    }

    @Override
    public List<ImageDto> getThumbNailsByPage(OffsetDateTime firstDate, OffsetDateTime lastDate, Pageable page) {
        return null;
    }

}
