package com.gs.photos.workflow.metadata.tiff;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public final class SignedRationalField extends RationalField {
    private static final long serialVersionUID = 1L;

    public SignedRationalField(
        Tag ifdTagParent,
        Tag tag,
        SimpleAbstractField<int[]> underLayingField,
        short tagValue
    ) {
        super(ifdTagParent,
            tag,
            underLayingField,
            tagValue);
    }
}