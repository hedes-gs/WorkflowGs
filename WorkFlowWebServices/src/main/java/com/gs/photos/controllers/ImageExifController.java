package com.gs.photos.controllers;

import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.gs.photos.repositories.IImageExifRepository;
import com.gs.photos.web.assembler.ImageExifAssembler;
import com.workflow.model.dtos.ExifDTO;
import com.workflow.model.dtos.ImageExifDto;
import com.workflow.model.dtos.ImageKeyDto;

@RestController
@RequestMapping("/api/gs")
@CrossOrigin(origins = "*")
public class ImageExifController {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ImageExifController.class);

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(ImageKeyDto.class, new ImageKeyDtoTypeEditor());
    }

    @Autowired
    protected ImageExifAssembler   imageExifAssembler;

    @Autowired
    protected IImageExifRepository imageExifRepository;

    @GetMapping("/exifs/{id}/{creationDate}/{version}")
    public @ResponseBody CollectionModel<EntityModel<ExifDTO>> getExifsByImageId(
        @PathVariable String id,
        @PathVariable OffsetDateTime creationDate,
        @PathVariable int version
    ) {
        ImageExifController.LOGGER.info("Retrieve exifs for  {}", id);
        ImageExifDto imageExifDto = this.imageExifRepository.findById(creationDate, id, version)
            .orElseThrow();
        return this.imageExifAssembler.toModel(imageExifDto);
    }
}
