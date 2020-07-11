package com.gs.photos.workflow.metadata.tiff;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

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