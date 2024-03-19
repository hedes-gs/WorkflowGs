package com.gs.photos.workflow.extimginfo.metadata.fields;

import java.io.IOException;

import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;
import com.gs.photos.workflow.extimginfo.metadata.tiff.ShortField;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;

public class SimpleShortField extends SimpleAbstractField<short[]> {

    protected SimpleShortField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    protected int     currentLength;
    protected short[] data;

    @Override
    public TiffField<short[]> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<short[]> shortField = new ShortField(ifdParent, tag, this, tagValue);
        return shortField;
    }

    @Override
    public short[] getData() { return this.data; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        short[] sdata = new short[this.getFieldLength()];
        try {
            if (sdata.length == 1) {
                rin.position(this.offset);
                sdata[0] = rin.readShort();
                this.currentLength = 4;
            } else if (sdata.length == 2) {
                rin.position(this.offset);
                sdata[0] = rin.readShort();
                this.currentLength += 2;
                rin.position(this.offset + this.currentLength);
                sdata[1] = rin.readShort();
                this.currentLength += 2;
            } else {
                rin.position(this.offset);
                int toOffset = rin.readInt();
                this.currentLength += 4;
                for (int j = 0; j < sdata.length; j++) {
                    rin.position(toOffset);
                    sdata[j] = rin.readShort();
                    toOffset += 2;
                }
            }
            this.data = sdata;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.currentLength; }

}
