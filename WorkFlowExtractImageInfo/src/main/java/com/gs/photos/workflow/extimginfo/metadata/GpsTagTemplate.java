package com.gs.photos.workflow.extimginfo.metadata;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class GpsTagTemplate extends AbstractTemplateTag {

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {
        if (this.data.getData()[0] != 0) {
            super.createSimpleTiffFields(rin, this.data.getData()[0], ifdContext);
        }
    }

    public GpsTagTemplate(
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
