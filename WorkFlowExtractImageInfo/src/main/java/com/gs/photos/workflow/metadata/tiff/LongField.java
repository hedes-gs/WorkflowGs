package com.gs.photos.workflow.metadata.tiff;

import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public final class LongField extends TiffField<int[]> {
    private static final long serialVersionUID = 1L;

    public LongField(
        Tag tag,
        SimpleAbstractField<int[]> underLayingField,
        short tagValue
    ) {
        super(tag,
            underLayingField,
            tagValue,
            underLayingField.getOffset());
    }

    @Override
    public String getDataAsString() { return StringUtils.longArrayToString(this.getData(), 0, 10, true); }
}