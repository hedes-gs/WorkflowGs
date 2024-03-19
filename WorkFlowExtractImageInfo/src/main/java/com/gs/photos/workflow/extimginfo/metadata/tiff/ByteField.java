package com.gs.photos.workflow.extimginfo.metadata.tiff;

import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.StringUtils;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public final class ByteField extends TiffField<byte[]> {
    private static final long serialVersionUID = 1L;

    public ByteField(
        Tag ifdTagParent,
        Tag tag,
        SimpleAbstractField<byte[]> underLayingField,
        short tagValue
    ) {
        super(ifdTagParent,
            tag,
            underLayingField,
            tagValue,
            underLayingField.getOffset());
    }

    @Override
    public String getDataAsString() { return StringUtils.byteArrayToHexString(this.underLayingField.getData(), 0, 10); }
}