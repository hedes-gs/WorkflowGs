package com.gs.photos.web.assembler;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.server.mvc.RepresentationModelAssemblerSupport;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import com.gs.photos.controllers.ImageController;
import com.gs.photos.controllers.ImageExifController;
import com.workflow.model.dtos.ExifDTO;

public class ExifAssembler extends RepresentationModelAssemblerSupport<ExifDTO, EntityModel<ExifDTO>> {

    static Class<EntityModel<ExifDTO>> getClassForConstructor() {
        EntityModel<ExifDTO> retValue = new EntityModel<ExifDTO>(new ExifDTO(), new Link[] {});
        Class<EntityModel<ExifDTO>> classA = (Class<EntityModel<ExifDTO>>) retValue.getClass();
        return classA;
    }

    public ExifAssembler() { super(ImageExifController.class,
        ExifAssembler.getClassForConstructor()); }

    @Override
    public EntityModel<ExifDTO> toModel(ExifDTO entity) {
        return new EntityModel<ExifDTO>(entity,
            WebMvcLinkBuilder.linkTo(
                WebMvcLinkBuilder.methodOn(ImageController.class, entity.getImageOwner())
                    .getImageById(
                        entity.getImageOwner()
                            .getImageId(),
                        entity.getImageOwner()
                            .getCreationDate(),
                        entity.getImageOwner()
                            .getVersion()))
                .withSelfRel());
    }

}
