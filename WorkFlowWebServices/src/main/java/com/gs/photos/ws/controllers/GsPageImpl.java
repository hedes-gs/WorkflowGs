package com.gs.photos.ws.controllers;

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.workflow.model.dtos.ImageDto;

public class GsPageImpl extends PageImpl<ImageDto> {

    public GsPageImpl(List<ImageDto> content) { super(content); }

    public GsPageImpl(
        List<ImageDto> content,
        Pageable pageable,
        long total
    ) { super(content,
        pageable,
        total); }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

}
