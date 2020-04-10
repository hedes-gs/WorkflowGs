package com.gs.photos.workflow.metadata.tiff;

import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public final class ByteField extends TiffField<byte[]> {
    private static final long serialVersionUID = 1L;

    public ByteField(
        Tag tag,
        SimpleAbstractField<byte[]> underLayingField,
        short tagValue
    ) {
        super(tag,
            underLayingField,
            tagValue,
            underLayingField.getOffset());
    }

    @Override
    public String getDataAsString() { return StringUtils.byteArrayToHexString(this.underLayingField.getData(), 0, 10); }
}