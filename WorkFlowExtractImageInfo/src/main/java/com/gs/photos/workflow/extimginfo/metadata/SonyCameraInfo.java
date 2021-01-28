package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class SonyCameraInfo extends AbstractSonySubdirectory {
    private Logger LOGGER = LoggerFactory.getLogger(SonyCameraInfo.class);

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        int offset = this.data.getOffset();
        this.LOGGER.debug(" SonyFocusInfoTemplate , offset is {}", offset);
        rin.position(offset);
        try {
            int index = 0;
            this.addBytesField(rin, ifdContext, offset, 8, "LensSpec", (short) 0xffe4, index++);
            this.addByteField(rin, ifdContext, offset + 20, "FocusModeSetting", (short) 0xffe1, index++);
            this.addByteField(rin, ifdContext, offset + 21, "AFPointSelected", (short) 0xffe2, index++);
            this.addByteField(rin, ifdContext, offset + 25, "AFPoint", (short) 0xffe3, index++);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public SonyCameraInfo(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<byte[]> data,
        IExifService exifService
    ) {
        super(tag,
            ifdParent,
            data,
            exifService);
    }

}
