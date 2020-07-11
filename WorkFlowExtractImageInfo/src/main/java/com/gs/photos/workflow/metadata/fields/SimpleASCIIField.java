package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.tiff.ASCIIField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleASCIIField extends SimpleAbstractField<String> {

    protected String data;

    public SimpleASCIIField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<String> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<String> ascIIField = new ASCIIField(ifdParent, tag, this, tagValue);
        return ascIIField;
    }

    @Override
    public String getData() { return this.data; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        byte[] data = new byte[this.getFieldLength()];
        try {
            if (data.length <= 4) {
                rin.position(this.offset);
                rin.readFully(data, 0, data.length);
            } else {
                rin.position(this.offset);
                rin.position(rin.readInt());
                rin.readFully(data, 0, data.length);
            }
            this.data = new String(data, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + 4; }

}
