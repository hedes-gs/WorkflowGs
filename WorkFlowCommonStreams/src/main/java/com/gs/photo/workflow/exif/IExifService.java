package com.gs.photo.workflow.exif;

import com.workflow.model.dtos.ExifDTO;

public interface IExifService {

    static final short IFD_IMAGE                    = (short) 0;
    static final short IFD_OTHER                    = (short) 1;
    static final short EXIF_SUB_IFD                 = (short) 0x8769;
    static final short GPS_SUB_IFD                  = (short) 0x8825;
    static final short EXIF_INTEROPERABILITY_OFFSET = (short) 0xA005;
    static final short SUB_IFDS                     = (short) 0x014A;
    static final short EXIF_PRIVATE_TAGS            = (short) 0xc634;
    static final short EXIF_SONY                    = (short) 0x927c;

    public ExifDTO.Builder getExifDTOFrom(
        ExifDTO.Builder builder,
        short ifdParent,
        short tag,
        int[] valueAsInt,
        short[] valueAsshort,
        byte[] valueAsByte
    );

    public Tag getTagFrom(short ifdParent, short tag);

    public String toString(Tag ifdTagParent, Tag tag, Object data);

    public String toString(short ifdTagParent, Tag tag, Object data);

    public String toString(FieldType fieldType, Object data);

}