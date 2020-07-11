package com.gs.photos.web.assembler;

import org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.stereotype.Component;

import com.workflow.model.dtos.ImageDto;

@Component
public class PageImageAssembler extends PagedResourcesAssembler<ImageDto> {

    public PageImageAssembler(HateoasPageableHandlerMethodArgumentResolver resolver) {
        super(resolver,
            null);
        this.setForceFirstAndLastRels(true);
    }

}