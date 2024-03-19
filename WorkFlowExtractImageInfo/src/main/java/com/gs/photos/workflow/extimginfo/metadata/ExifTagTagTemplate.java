package com.gs.photos.workflow.extimginfo.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class ExifTagTagTemplate extends AbstractTemplateTag {
    private Logger                       LOGGER = LogManager.getLogger(ExifTagTagTemplate.class);

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        if (this.data.getData()[0] != 0) {
            super.createSimpleTiffFields(rin, this.data.getData()[0], ifdContext);
        }

    }

    public ExifTagTagTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<int[]> data,
        IExifService exifService
    ) {
        super(tag,
            ifdParent,
            exifService);
        this.data = data;
    }

}
