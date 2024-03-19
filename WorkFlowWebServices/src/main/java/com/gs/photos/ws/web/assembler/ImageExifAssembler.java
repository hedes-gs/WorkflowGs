package com.gs.photos.ws.web.assembler;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.LinkRelation;
import org.springframework.hateoas.server.mvc.RepresentationModelAssemblerSupport;
import org.springframework.hateoas.server.reactive.WebFluxLinkBuilder;
import org.springframework.stereotype.Component;

import com.gs.photos.ws.controllers.ImageController;
import com.gs.photos.ws.controllers.ImageExifController;
import com.workflow.model.dtos.ExifDTO;
import com.workflow.model.dtos.ImageExifDto;

@Component
public class ImageExifAssembler
    extends RepresentationModelAssemblerSupport<ImageExifDto, CollectionModel<EntityModel<ExifDTO>>> {

    public ImageExifAssembler() { super(ImageExifController.class,
        ImageExifAssembler.getClassForConstructor()); }

    static Class<CollectionModel<EntityModel<ExifDTO>>> getClassForConstructor() {
        CollectionModel<EntityModel<ExifDTO>> retValue = new CollectionModel<EntityModel<ExifDTO>>(
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST);
        Class<CollectionModel<EntityModel<ExifDTO>>> classA = (Class<CollectionModel<EntityModel<ExifDTO>>>) retValue
            .getClass();
        return classA;
    }

    @Override
    public CollectionModel<EntityModel<ExifDTO>> toModel(ImageExifDto entity) {
        List<ExifDTO> exifs = entity.getExifs();
        final List<EntityModel<ExifDTO>> listOfEntitiesModel = exifs.stream()
            .map(
                (
                    t) -> new EntityModel<>(t,
                        WebFluxLinkBuilder
                            .linkTo(
                                WebFluxLinkBuilder.methodOn(ImageController.class, entity.getImageOwner())
                                    .getImageById(
                                        entity.getImageOwner()
                                            .getSalt(),
                                        entity.getImageOwner()
                                            .getImageId(),
                                        entity.getImageOwner()
                                            .getCreationDate(),
                                        entity.getImageOwner()
                                            .getVersion()))
                            .withSelfRel()
                            .toMono()
                            .block()))
            .collect(Collectors.toList());

        CollectionModel<EntityModel<ExifDTO>> retValue = new CollectionModel<EntityModel<ExifDTO>>(listOfEntitiesModel,
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getImageWithMediaType(
                        entity.getImageOwner()
                            .getSalt(),
                        entity.getImageOwner()
                            .getImageId(),
                        entity.getImageOwner()
                            .getCreationDate(),
                        entity.getImageOwner()
                            .getVersion()))
                .withRel(LinkRelation.of("_img"))
                .toMono()
                .block(),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getNextImageById(
                        entity.getImageOwner()
                            .getSalt(),
                        entity.getImageOwner()
                            .getImageId(),
                        entity.getImageOwner()
                            .getCreationDate(),
                        entity.getImageOwner()
                            .getVersion()))
                .withRel(LinkRelation.of("_next"))
                .toMono()
                .block(),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getPreviousImageById(
                        entity.getImageOwner()
                            .getSalt(),
                        entity.getImageOwner()
                            .getImageId(),
                        entity.getImageOwner()
                            .getCreationDate(),
                        entity.getImageOwner()
                            .getVersion()))
                .withRel(LinkRelation.of("_prev"))
                .toMono()
                .block(),
            WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class, entity.getImageOwner())
                    .getImageById(
                        entity.getImageOwner()
                            .getSalt(),
                        entity.getImageOwner()
                            .getImageId(),
                        entity.getImageOwner()
                            .getCreationDate(),
                        entity.getImageOwner()
                            .getVersion()))
                .withSelfRel()
                .toMono()
                .block());

        return retValue;
    }

}
