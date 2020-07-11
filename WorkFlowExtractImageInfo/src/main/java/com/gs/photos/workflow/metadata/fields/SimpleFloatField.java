package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.tiff.FloatField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleFloatField extends SimpleAbstractField<float[]> {

    protected float[] data;
    protected int     currentLength;

    public SimpleFloatField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<float[]> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<float[]> floatField = new FloatField(ifdParent, tag, this, tagValue);
        return floatField;
    }

    @Override
    public float[] getData() { return null; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        try {
            this.data = new float[this.getFieldLength()];
            if (this.data.length == 1) {
                rin.position(this.offset);
                this.data[0] = rin.readFloat();
                this.currentLength = 4;
            } else {
                rin.position(this.offset);
                int toOffset = rin.readInt();
                this.currentLength = 4;
                for (int j = 0; j < this.data.length; j++) {
                    rin.position(toOffset);
                    this.data[j] = rin.readFloat();
                    toOffset += 4;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.currentLength; }

}
