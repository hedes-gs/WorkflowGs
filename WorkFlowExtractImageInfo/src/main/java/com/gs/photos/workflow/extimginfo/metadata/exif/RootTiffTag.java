package com.gs.photos.workflow.extimginfo.metadata.exif;

import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.StringUtils;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffTag.Attribute;

public enum RootTiffTag implements Tag {

    IFD0(
        "IFD0", (short) 0x0000, Attribute.BASELINE
    ), IFD1(
        "IFD1", (short) 0x0001, Attribute.BASELINE
    ), IFD2(
        "IFD2", (short) 0x0002, Attribute.BASELINE
    ), IFD3(
        "IFD3", (short) 0x0003, Attribute.BASELINE
    );

    private final String    name;

    private final short     value;

    private final Attribute attribute;

    @Override
    public String toString() {
        return this.name + " [Value: " + StringUtils.shortToHexStringMM(this.value) + "] (" + this.getAttribute() + ")";
    }

    public Attribute getAttribute() { return this.attribute; }

    private RootTiffTag(
        String name,
        short value,
        Attribute attribute
    ) {
        this.name = name;
        this.value = value;
        this.attribute = attribute;
    }

    @Override
    public FieldType getFieldType() { // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getName() { // TODO Auto-generated method stub
        return this.name;
    }

    @Override
    public short getValue() { // TODO Auto-generated method stub
        return this.value;
    }

}
