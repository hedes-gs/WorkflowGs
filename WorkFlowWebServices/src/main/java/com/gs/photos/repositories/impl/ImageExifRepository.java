package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.repositories.IImageExifRepository;
import com.workflow.model.dtos.ImageExifDto;

@Repository
public class ImageExifRepository implements IImageExifRepository {

    @Autowired
    protected HbaseExifDataDAO hbaseExifDataDAO;

    @Override
    public Optional<ImageExifDto> findById(OffsetDateTime creationDate, String imageId, int version) {

        Optional<ImageExifDto> retValue = Optional.empty();
        try {
            retValue = Optional.of(this.hbaseExifDataDAO.get(creationDate, imageId, version));
        } catch (IOException e) {

            e.printStackTrace();
        }
        return retValue;
    }

}
