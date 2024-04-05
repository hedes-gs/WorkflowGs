package com.gs.photo.workflow.pubthumbimages;

import com.gs.photo.common.workflow.IStream;

public interface IBeanPublishThumbImages extends IStream {
    public static final int                 JOIN_WINDOW_TIME           = 86400;
    public static final short               EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short               EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short               EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short               EXIF_ORIENTATION           = (short) 0x0112;

    public static final short               SONY_EXIF_LENS             = (short) 0xB027;
    public static final short               EXIF_LENS                  = (short) 0xA434;
    public static final short               EXIF_FOCAL_LENS            = (short) 0x920A;
    public static final short               EXIF_SHIFT_EXPO            = (short) 0x9204;
    public static final short               EXIF_SPEED_ISO             = (short) 0x8827;
    public static final short               EXIF_APERTURE              = (short) 0x829D;
    public static final short               EXIF_SPEED                 = (short) 0x829A;
    public static final short               EXIF_COPYRIGHT             = (short) 0x8298;
    public static final short               EXIF_ARTIST                = (short) 0x13B;
    public static final short               EXIF_CAMERA_MODEL          = (short) 0x110;

    public static final short[]             EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[]             EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

    public static final short[]             EXIF_LENS_PATH             = { (short) 0, (short) 0x8769 };
    public static final short[]             SONY_EXIF_LENS_PATH        = { (short) 0, (short) 0x8769, (short) 0x927c };
    public static final short[]             EXIF_FOCAL_LENS_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[]             EXIF_SHIFT_EXPO_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[]             EXIF_SPEED_ISO_PATH        = { (short) 0, (short) 0x8769 };
    public static final short[]             EXIF_APERTURE_PATH         = { (short) 0, (short) 0x8769 };
    public static final short[]             EXIF_SPEED_PATH            = { (short) 0, (short) 0x8769 };

    public static final short[]             EXIF_COPYRIGHT_PATH        = { (short) 0 };
    public static final short[]             EXIF_ARTIST_PATH           = { (short) 0 };
    public static final short[]             EXIF_CAMERA_MODEL_PATH     = { (short) 0 };
    public static final short[]             EXIF_ORIENTATION_PATH      = { (short) 0 };
}
