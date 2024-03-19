package com.gs.photos.ws.repositories;

import java.time.OffsetDateTime;
import java.util.Optional;

import com.workflow.model.dtos.ImageExifDto;

public interface IImageExifRepository {

    Optional<ImageExifDto> findById(short salt, OffsetDateTime creationDate, String id, int version);

}
