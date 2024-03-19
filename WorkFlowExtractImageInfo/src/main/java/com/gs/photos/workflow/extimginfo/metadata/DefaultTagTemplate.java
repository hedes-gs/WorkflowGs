package com.gs.photos.workflow.extimginfo.metadata;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;

public class DefaultTagTemplate extends AbstractTemplateTag {

    public DefaultTagTemplate(
        Tag tag,
        IFD parent,
        IExifService exifService
    ) { super(tag,
        new IFD(tag),
        exifService); }

    public DefaultTagTemplate(
        Tag tag,
        IExifService exifService
    ) { super(tag,
        new IFD(tag),
        exifService); }

}
