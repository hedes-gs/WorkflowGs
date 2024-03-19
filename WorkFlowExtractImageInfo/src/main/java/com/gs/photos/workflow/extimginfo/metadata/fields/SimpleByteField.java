package com.gs.photos.workflow.extimginfo.metadata.fields;

import java.io.IOException;

import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;
import com.gs.photos.workflow.extimginfo.metadata.tiff.ByteField;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;

public class SimpleByteField extends SimpleAbstractField<byte[]> {

    private org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SimpleByteField.class);

    protected byte[]         data;
    protected int            currentLength;

    public SimpleByteField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

    @Override
    public TiffField<byte[]> createTiffField(Tag ifdParent, Tag tag, short tagValue) {
        TiffField<byte[]> byteField = new ByteField(ifdParent, tag, this, tagValue);
        return byteField;
    }

    @Override
    public byte[] getData() {
        // TODO Auto-generated method stub
        return this.data;
    }

    @Override
    public void updateData(FileChannelDataInput rin) {
        try {
            this.data = new byte[this.getFieldLength()];
            rin.position(this.offset);
            if (this.data.length <= 4) {
                rin.readFully(this.data, 0, this.data.length);
            } else {
                int offset = rin.readInt();
                rin.position(offset);
                rin.readFully(this.data, 0, this.data.length);
            }
            this.currentLength = 4;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getNextOffset() { return this.offset + this.currentLength; }

}
