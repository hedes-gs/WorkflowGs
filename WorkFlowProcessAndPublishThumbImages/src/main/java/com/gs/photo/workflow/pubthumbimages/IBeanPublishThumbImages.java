package com.gs.photo.workflow.pubthumbimages;

import java.util.Map;

import com.gs.photo.common.workflow.IStream;

public interface IBeanPublishThumbImages extends IStream {
    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short   EXIF_ORIENTATION           = (short) 0x0112;

    public static final short   SONY_EXIF_LENS             = (short) 0xB027;
    public static final short   EXIF_LENS                  = (short) 0xA434;
    public static final short   EXIF_FOCAL_LENS            = (short) 0x920A;
    public static final short   EXIF_SHIFT_EXPO            = (short) 0x9204;
    public static final short   EXIF_SPEED_ISO             = (short) 0x8827;
    public static final short   EXIF_APERTURE              = (short) 0x829D;
    public static final short   EXIF_SPEED                 = (short) 0x829A;
    public static final short   EXIF_COPYRIGHT             = (short) 0x8298;
    public static final short   EXIF_ARTIST                = (short) 0x13B;
    public static final short   EXIF_CAMERA_MODEL          = (short) 0x110;

    public Map<Short, String>   ALL_EXIFS                  = Map.ofEntries(
        Map.entry((short) 0x9003, "EXIF_CREATION_DATE_ID"),
        Map.entry((short) 0xA002, "EXIF_SIZE_WIDTH"),
        Map.entry((short) 0xA003, "EXIF_SIZE_HEIGHT"),
        Map.entry((short) 0x0112, "EXIF_ORIENTATION"),

        Map.entry((short) 0xB027, "SONY_EXIF_LENS"),
        Map.entry((short) 0xA434, "EXIF_LENS"),
        Map.entry((short) 0x920A, "EXIF_FOCAL_LENS"),
        Map.entry((short) 0x9204, "EXIF_SHIFT_EXPO"),
        Map.entry((short) 0x8827, "EXIF_SPEED_ISO"),
        Map.entry((short) 0x829D, "EXIF_APERTURE"),
        Map.entry((short) 0x829A, "EXIF_SPEED"),
        Map.entry((short) 0x8298, "EXIF_COPYRIGHT"),
        Map.entry((short) 0x13B, "EXIF_ARTIST"),
        Map.entry((short) 0x110, "EXIF_CAMERA_MODEL"));

    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

    public static final short[] EXIF_LENS_PATH             = { (short) 0, (short) 0x8769 };
    public static final short[] SONY_EXIF_LENS_PATH        = { (short) 0, (short) 0x8769, (short) 0x927c };
    public static final short[] EXIF_FOCAL_LENS_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SHIFT_EXPO_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SPEED_ISO_PATH        = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_APERTURE_PATH         = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SPEED_PATH            = { (short) 0, (short) 0x8769 };

    public static final short[] EXIF_COPYRIGHT_PATH        = { (short) 0 };
    public static final short[] EXIF_ARTIST_PATH           = { (short) 0 };
    public static final short[] EXIF_CAMERA_MODEL_PATH     = { (short) 0 };
    public static final short[] EXIF_ORIENTATION_PATH      = { (short) 0 };
}
