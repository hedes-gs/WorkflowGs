package com.gs.photos.workflow.extimginfo.metadata.fields;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;

public class SimpleUndefinedField extends SimpleByteField {

    protected static Logger LOGGER = LoggerFactory.getLogger(SimpleUndefinedField.class);

    int                     currentOffset;

    @Override
    public int getOffset() { return this.currentOffset; }

    @Override
    public void updateData(FileChannelDataInput rin) {
        try {
            this.data = new byte[this.getFieldLength()];
            rin.position(this.offset);
            if (this.data.length <= 4) {
                rin.readFully(this.data, 0, this.data.length);
            } else {
                int offset = rin.readInt();
                this.currentOffset = offset;
                rin.position(offset);
                rin.readFully(this.data, 0, this.data.length);
            }
            this.currentLength = 4;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SimpleUndefinedField(
        int fieldLength,
        int offset,
        short type
    ) { super(fieldLength,
        offset,
        type); }

}
