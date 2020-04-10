package com.gs.photos.workflow.metadata;

import com.gs.photos.workflow.metadata.exif.InteropTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class InteropTagTemplate extends AbstractTemplateTag {

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin) {
        if (this.data.getData()[0] != 0) {
            super.createSimpleTiffFields(rin, this.data.getData()[0]);
        }
    }

    public InteropTagTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<int[]> data
    ) {
        super(tag,
            ifdParent);
        this.data = data;
    }

    @Override
    public Tag convertTagValueToTag(short tag) { return InteropTag.fromShort(tag); }

}
