package com.gs.photos.workflow.metadata.fields;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public abstract class SimpleAbstractField<T> {

    protected final int   fieldLength;
    protected final int   offset;
    protected final short type;

    public int getFieldLength() { return this.fieldLength; }

    protected SimpleAbstractField(
        int fieldLength,
        int offset,
        short type
    ) {
        this.fieldLength = fieldLength;
        this.offset = offset;
        this.type = type;
    }

    public int getOffset() { return this.offset; }

    public abstract TiffField<T> createTiffField(Tag ifdParent, Tag tag, short tagValue);

    public short getType() { return this.type; }

    public abstract T getData();

    public abstract void updateData(FileChannelDataInput rin);

    public abstract int getNextOffset();

}
