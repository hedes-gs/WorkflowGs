package com.gs.photos.workflow.metadata;

import com.gs.photos.workflow.metadata.IFD.IFDContext;
import com.gs.photos.workflow.metadata.exif.GPSTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class GpsTagTemplate extends AbstractTemplateTag {

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {
        System.out.println("... buildChildren in  GpsTagTemplate ");

        if (this.data.getData()[0] != 0) {
            super.createSimpleTiffFields(rin, this.data.getData()[0], ifdContext);
        }
    }

    public GpsTagTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<int[]> data
    ) {
        super(tag,
            ifdParent);
        this.data = data;
    }

    @Override
    public Tag convertTagValueToTag(short tag) { return GPSTag.fromShort(tag); }

}
