package com.gs.photos.repositories;

import java.time.OffsetDateTime;
import java.util.Optional;

import com.workflow.model.dtos.ImageExifDto;

public interface IImageExifRepository {

    Optional<ImageExifDto> findById(OffsetDateTime creationDate, String id, int version);

}
