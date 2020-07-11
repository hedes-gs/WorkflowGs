package com.gs.photos.workflow.metadata.tiff;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class RationalField extends TiffField<int[]> {
    private static final long serialVersionUID = 1L;

    public RationalField(
        Tag ifdTagParent,
        Tag tag,
        SimpleAbstractField<int[]> underLayingField,
        short tagValue
    ) {
        super(ifdTagParent,
            tag,
            underLayingField,
            tagValue,
            underLayingField.getOffset());
    }

    @Override
    public String getDataAsString() { return StringUtils.longArrayToString(this.getData(), 0, 10, true); }
}