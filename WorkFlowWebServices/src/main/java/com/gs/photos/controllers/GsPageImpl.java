package com.gs.photos.controllers;

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;

public class GsPageImpl extends PageImpl<ImageDto> {

    protected ImageKeyDto first;
    protected ImageKeyDto last;

    public ImageKeyDto getFirst() { return this.first; }

    public void setFirst(ImageKeyDto first) { this.first = first; }

    public ImageKeyDto getLast() { return this.last; }

    public void setLast(ImageKeyDto last) { this.last = last; }

    public GsPageImpl(List<ImageDto> content) { super(content); }

    public GsPageImpl(
        List<ImageDto> content,
        Pageable pageable,
        long total
    ) {
        super(content,
            pageable,
            total);
        this.first = content.get(0)
            .getData();
        this.last = content.get(content.size() - 1)
            .getData();
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

}
