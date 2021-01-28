package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.gs.photos.ws.repositories.IImageExifRepository;
import com.workflow.model.dtos.ImageExifDto;

@Repository
public class ImageExifRepository implements IImageExifRepository {

    @Autowired
    protected HbaseExifDataDAO hbaseExifDataDAO;

    @Override
    public Optional<ImageExifDto> findById(short salt, OffsetDateTime creationDate, String imageId, int version) {

        Optional<ImageExifDto> retValue = Optional.empty();
        try {
            retValue = Optional.of(this.hbaseExifDataDAO.get(salt, creationDate, imageId, version));
        } catch (IOException e) {

            e.printStackTrace();
        }
        return retValue;
    }

}
