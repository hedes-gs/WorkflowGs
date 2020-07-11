package com.gs.photos.web.assembler;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.LinkRelation;
import org.springframework.hateoas.server.mvc.RepresentationModelAssemblerSupport;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.stereotype.Component;

import com.gs.photos.controllers.ImageController;
import com.gs.photos.controllers.ImageExifController;
import com.workflow.model.dtos.ImageDto;;

@Component
public class ImageAssembler extends RepresentationModelAssemblerSupport<ImageDto, EntityModel<ImageDto>> {

    static Class<EntityModel<ImageDto>> getClassForConstructor() {
        EntityModel<ImageDto> retValue = new EntityModel<ImageDto>(new ImageDto(), new Link[] {});
        Class<EntityModel<ImageDto>> classA = (Class<EntityModel<ImageDto>>) retValue.getClass();
        return classA;
    }

    public ImageAssembler() { super(ImageController.class,
        ImageAssembler.getClassForConstructor()); }

    @Override
    public EntityModel<ImageDto> toModel(ImageDto entity) {
        return new EntityModel<ImageDto>(entity,

            WebMvcLinkBuilder.linkTo(
                WebMvcLinkBuilder.methodOn(ImageController.class)
                    .getImageWithMediaType(
                        entity.getData()
                            .getImageId(),
                        entity.getData()
                            .getCreationDate(),
                        entity.getData()
                            .getVersion()))
                .withRel(LinkRelation.of("_img")),
            WebMvcLinkBuilder.linkTo(
                WebMvcLinkBuilder.methodOn(ImageController.class)
                    .getImageById(
                        entity.getData()
                            .getImageId(),
                        entity.getData()
                            .getCreationDate(),
                        entity.getData()
                            .getVersion()))
                .withSelfRel(),
            WebMvcLinkBuilder.linkTo(
                WebMvcLinkBuilder.methodOn(ImageController.class)
                    .updateRating(entity))
                .withRel(LinkRelation.of("_upd")),
            WebMvcLinkBuilder.linkTo(
                WebMvcLinkBuilder.methodOn(ImageExifController.class)
                    .getExifsByImageId(
                        entity.getData()
                            .getImageId(),
                        entity.getData()
                            .getCreationDate(),
                        entity.getData()
                            .getVersion()))
                .withRel(LinkRelation.of("_exif")));
    }

}
