package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;
import java.util.Arrays;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.RationalField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleRationalField extends SimpleAbstractField<int[]> {

    private int[] data;
    private int   currentLength;

    public SimpleRationalField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<int[]> createTiffField(Tag tag, short tagValue) {
        TiffField<int[]> ratField = new RationalField(tag, this, tagValue);
        return ratField;
    }

    @Override
    public int[] getData() { return this.data; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        try {
            int len = 2 * this.getFieldLength();
            this.data = new int[len];
            rin.position(this.offset);
            int toOffset = rin.readInt();
            this.currentLength = 4;
            for (int j = 0; j < len; j += 2) {
                rin.position(toOffset);
                this.data[j] = rin.readInt();
                toOffset += 4;
                rin.position(toOffset);
                this.data[j + 1] = rin.readInt();
                toOffset += 4;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.currentLength; }

    @Override
    public String toString() {
        return "SimpleRationalField [data=" + Arrays.toString(this.data) + ", currentLength=" + this.currentLength
            + "]";
    }

}
