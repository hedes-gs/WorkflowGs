package com.gs.photos.workflow.extimginfo.metadata.tiff;

import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public final class ASCIIField extends TiffField<String> {
    private static final long serialVersionUID = 1L;

    public ASCIIField(
        Tag ifdTagParent,
        Tag tag,
        SimpleAbstractField<String> data,
        short tagValue
    ) {
        super(ifdTagParent,
            tag,
            data,
            tagValue,
            data.getOffset());
    }

    @Override
    public String getDataAsString() { return this.underLayingField.getData(); }
}