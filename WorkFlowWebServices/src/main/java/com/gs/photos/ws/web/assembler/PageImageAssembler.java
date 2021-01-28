package com.gs.photos.ws.web.assembler;

import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import com.workflow.model.dtos.ImageDto;

@Component
public class PageImageAssembler extends PagedResourcesAssembler<ImageDto> {

    public PageImageAssembler() {
        super(null,
            UriComponentsBuilder.fromOriginHeader("/api")
                .build());
        this.setForceFirstAndLastRels(true);
    }

}