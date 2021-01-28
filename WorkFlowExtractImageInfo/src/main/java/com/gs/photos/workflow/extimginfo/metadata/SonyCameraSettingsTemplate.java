package com.gs.photos.workflow.extimginfo.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class SonyCameraSettingsTemplate extends AbstractSonySubdirectory {
    private Logger LOGGER = LoggerFactory.getLogger(SonyCameraSettingsTemplate.class);

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        int offset = this.data.getOffset();
        this.LOGGER.debug(" SonyFocusInfoTemplate , offset is {}", offset);

    }

    public SonyCameraSettingsTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<byte[]> data,
        IExifService exifService
    ) {
        super(tag,
            ifdParent,
            data,
            exifService);
    }

}
