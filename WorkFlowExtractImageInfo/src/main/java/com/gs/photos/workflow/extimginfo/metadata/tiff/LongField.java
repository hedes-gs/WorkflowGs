package com.gs.photos.workflow.extimginfo.metadata.tiff;

import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.StringUtils;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public final class LongField extends TiffField<int[]> {
    private static final long serialVersionUID = 1L;

    public LongField(
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