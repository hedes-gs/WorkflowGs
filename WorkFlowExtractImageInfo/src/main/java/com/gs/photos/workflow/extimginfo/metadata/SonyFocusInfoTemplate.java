package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class SonyFocusInfoTemplate extends AbstractSonySubdirectory {
    private Logger LOGGER = LoggerFactory.getLogger(SonyFocusInfoTemplate.class);

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        int offset = this.data.getOffset();
        this.LOGGER.debug(" SonyFocusInfoTemplate , offset is {}", offset);
        rin.position(offset);
        try {
            int index = 0;
            this.addByteField(rin, ifdContext, offset + 14, "Exif.Sony1.DriveMode2", (short) 0xffff, index++);
            this.addByteField(rin, ifdContext, offset + 16, "Exif.Sony1.Rotation", (short) 0xfffe, index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 20,
                "Exif.Sony1.ImageStabilizationSetting",
                (short) 0xfffd,
                index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 21,
                "Exif.Sony1.DynamicRangeOptimizerMode",
                (short) 0xfffc,
                index++);
            this.addByteField(rin, ifdContext, offset + 43, "Exif.Sony1.BracketShotNumber", (short) 0xfffb, index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 44,
                "Exif.Sony1.WhiteBalanceBracketing",
                (short) 0xfffa,
                index++);
            this.addByteField(rin, ifdContext, offset + 45, "Exif.Sony1.BracketShotNumber2", (short) 0xfff9, index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 46,
                "Exif.Sony1.DynamicRangeOptimizerBracket",
                (short) 0xfff8,
                index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 47,
                "Exif.Sony1.ExposureBracketShotNumber",
                (short) 0xfff7,
                index++);
            this.addByteField(rin, ifdContext, offset + 63, "Exif.Sony1.ExposureProgram", (short) 0xfff6, index++);
            this.addByteField(rin, ifdContext, offset + 65, "Exif.Sony1.CreativeStyle", (short) 0xfff5, index++);
            this.addByteField(rin, ifdContext, offset + 109, "Exif.Sony1.ISOSetting", (short) 0xfff4, index++);
            this.addByteField(rin, ifdContext, offset + 111, "Exif.Sony1.ISO", (short) 0xfff3, index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 119,
                "Exif.Sony1.DynamicRangeOptimizerMode",
                (short) 0xfff2,
                index++);
            this.addByteField(
                rin,
                ifdContext,
                offset + 119,
                "Exif.Sony1.DynamicRangeOptimizerLevel",
                (short) 0xfff1,
                index++);
            this.addByteField(rin, ifdContext, offset + 2491, "Exif.Sony1.FocusPosition", (short) 0xfff0, index++);
            this.addLongField(rin, ifdContext, offset + 2118, "Exif.Sony1.ShutterCount", (short) 0xffEF, index++);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public SonyFocusInfoTemplate(
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
