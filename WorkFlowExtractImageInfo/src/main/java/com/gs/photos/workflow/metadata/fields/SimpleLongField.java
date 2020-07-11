package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.tiff.LongField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleLongField extends SimpleAbstractField<int[]> {

    protected int   nextOffset;
    protected int[] data;

    public SimpleLongField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<int[]> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<int[]> longField = new LongField(ifdParent, tag, this, tagValue);
        return longField;
    }

    @Override
    public int[] getData() { return this.data; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        this.data = new int[this.getFieldLength()];
        try {
            if (this.data.length == 1) {
                rin.position(this.offset);
                this.data[0] = rin.readInt();
                this.nextOffset = 4;
            } else {
                rin.position(this.offset);
                int toOffset = rin.readInt();
                this.nextOffset = 4;
                for (int j = 0; j < this.data.length; j++) {
                    rin.position(toOffset);
                    this.data[j] = rin.readInt();
                    toOffset += 4;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.nextOffset; }

}
