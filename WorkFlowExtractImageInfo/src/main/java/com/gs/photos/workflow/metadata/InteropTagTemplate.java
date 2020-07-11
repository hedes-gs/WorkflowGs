package com.gs.photos.workflow.metadata;

import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.IFD.IFDContext;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class InteropTagTemplate extends AbstractTemplateTag {

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {
        if (this.data.getData()[0] != 0) {
            super.createSimpleTiffFields(rin, this.data.getData()[0], ifdContext);
        }
    }

    public InteropTagTemplate(
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
