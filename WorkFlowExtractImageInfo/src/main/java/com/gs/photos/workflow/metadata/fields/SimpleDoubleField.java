package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.tiff.DoubleField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleDoubleField extends SimpleAbstractField<double[]> {

    protected double[] data;
    protected int      currentLength;

    public SimpleDoubleField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<double[]> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<double[]> doubleField = new DoubleField(ifdParent, tag, this, tagValue);
        return doubleField;
    }

    @Override
    public double[] getData() { return this.data; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        try {
            this.data = new double[this.getFieldLength()];
            rin.position(this.offset);
            int toOffset = rin.readInt();
            this.currentLength = 4;
            for (int j = 0; j < this.data.length; j++) {
                rin.position(toOffset);
                this.data[j] = rin.readDouble();
                toOffset += 8;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.currentLength; }

}
