package com.workflow.model.exif;

import java.util.HashMap;
import java.util.Map;

public enum ExifAndTiffTag {
    EXPOSURE_TIME(
        "Exposure Time", (short) 0x829a
    ), FNUMBER(
        "FNumber", (short) 0x829d
    ),
    // EXIF_SUB_IFD("ExifSubIFD", (short)0x8769),
    EXPOSURE_PROGRAM(
        "Exposure Program", (short) 0x8822
    ), SPECTRAL_SENSITIVITY(
        "Spectral Sensitivity", (short) 0x8824
    ),
    // GPS_SUB_IFD("GPSSubIFD", (short)0x8825),
    ISO_SPEED_RATINGS(
        "ISO Speed Ratings", (short) 0x8827
    ), OECF(
        "OECF", (short) 0x8828
    ),

    EXIF_VERSION(
        "Exif Version", (short) 0x9000
    ), DATE_TIME_ORIGINAL(
        "DateTime Original", (short) 0x9003
    ), DATE_TIME_DIGITIZED(
        "DateTime Digitized", (short) 0x9004
    ),

    COMPONENT_CONFIGURATION(
        "Component Configuration", (short) 0x9101
    ), COMPRESSED_BITS_PER_PIXEL(
        "Compressed Bits PerPixel", (short) 0x9102
    ),

    SHUTTER_SPEED_VALUE(
        "Shutter Speed Value", (short) 0x9201
    ), APERTURE_VALUE(
        "Aperture Value", (short) 0x9202
    ), BRIGHTNESS_VALUE(
        "Bright Value", (short) 0x9203
    ), EXPOSURE_BIAS_VALUE(
        "Exposure Bias Value", (short) 0x9204
    ), MAX_APERTURE_VALUE(
        "Max Aperture Value", (short) 0x9205
    ), SUBJECT_DISTANCE(
        "Subject Distance", (short) 0x9206
    ), METERING_MODE(
        "Metering Mode", (short) 0x9207
    ), LIGHT_SOURCE(
        "Light Source", (short) 0x9208
    ), FLASH(
        "Flash", (short) 0x9209
    ), FOCAL_LENGTH(
        "Focal Length", (short) 0x920a
    ), SUBJECT_AREA(
        "Subject Area", (short) 0x9214
    ), MAKER_NODE(
        "Maker Note", (short) 0x927c
    ), USER_COMMENT(
        "User Comment", (short) 0x9286
    ), SUB_SEC_TIME(
        "Sub Sec Time", (short) 0x9290
    ), SUB_SEC_TIME_ORIGINAL(
        "Sub Sec Time Original", (short) 0x9291
    ), SUB_SEC_TIME_DIGITIZED(
        "Sub Sec Time Digitized", (short) 0x9292
    ), FLASH_PIX_VERSION(
        "Flash Pix Version", (short) 0xa000
    ), COLOR_SPACE(
        "Color Space", (short) 0xa001
    ), EXIF_IMAGE_WIDTH(
        "Exif Image Width", (short) 0xA002
    ), EXIF_IMAGE_HEIGHT(
        "Exif Image Height", (short) 0xA003
    ), RELATED_SOUND_FILE(
        "Related Sound File", (short) 0xA004
    ), EXIF_INTEROPERABILITY_OFFSET(
        "Exif Interoperability Offset", (short) 0xA005
    ), FLASH_ENERGY(
        "Flash Energy", (short) 0xa20b
    ), SPATIAL_FREQUENCY_RESPONSE(
        "Spatial Frequency Response", (short) 0xa20c
    ), FOCAL_PLANE_X_RESOLUTION(
        "Focal Plane XResolution", (short) 0xa20e
    ), FOCAL_PLANE_Y_RESOLUTION(
        "Focal Plane YResolution", (short) 0xa20f
    ), FOCAL_PLANE_RESOLUTION_UNIT(
        "Focal Plane Resolution Unit", (short) 0xa210
    ), SUBJECT_LOCATION(
        "Subject Location", (short) 0xa214
    ), EXPOSURE_INDEX(
        "Exposure Index", (short) 0xa215
    ), SENSING_METHOD(
        "Sensing Method", (short) 0xa217
    ), FILE_SOURCE(
        "File Source", (short) 0xa300
    ), SCENE_TYPE(
        "Scene Type", (short) 0xa301
    ), CFA_PATTERN(
        "CFA Pattern", (short) 0xa302
    ), CUSTOM_RENDERED(
        "Custom Rendered", (short) 0xa401
    ), EXPOSURE_MODE(
        "Exposure Mode", (short) 0xa402
    ), WHITE_BALENCE(
        "While Balence", (short) 0xa403
    ), DIGITAL_ZOOM_RATIO(
        "Digital Zoom Ratio", (short) 0xa404
    ), FOCAL_LENGTH_IN_35MM_FORMAT(
        "Focal Length In 35mm Format", (short) 0xa405
    ), SCENE_CAPTURE_TYPE(
        "Scene Capture Type", (short) 0xa406
    ), GAIN_CONTROL(
        "Gain Control", (short) 0xa407
    ), CONTRAST(
        "Contrast", (short) 0xa408
    ), SATURATION(
        "Saturation", (short) 0xa409
    ), SHARPNESS(
        "Sharpness", (short) 0xa40a
    ), DEVICE_SETTING_DESCRIPTION(
        "Device Setting Description", (short) 0xa40b
    ), SUBJECT_DISTANCE_RANGE(
        "Subject Distance Range", (short) 0xa40c
    ), IMAGE_UNIQUE_ID(
        "Image Unique ID", (short) 0xa420
    ), OWNER_NAME(
        "Owner Name", (short) 0xa430
    ), BODY_SERIAL_NUMBER(
        "Body Serial Number", (short) 0xa431
    ), LENS_SPECIFICATION(
        "Lens Specification", (short) 0xa432
    ), LENS_Make(
        "Lens Make", (short) 0xa433
    ), LENS_MODEL(
        "Lens Model", (short) 0xa434
    ), LENS_SERIAL_NUMBER(
        "Lens Serial Number", (short) 0xa435
    ), EXPAND_SOFTWARE(
        "Expand Software", (short) 0xafc0
    ), EXPAND_LENS(
        "Expand Lens", (short) 0xafc1
    ), EXPAND_FILM(
        "Expand Film", (short) 0xafc2
    ), EXPAND_FILTER_LENS(
        "Expand Filter Lens", (short) 0xafc3
    ), EXPAND_SCANNER(
        "Expand Scanner", (short) 0xafc4
    ), EXPAND_FLASH_LAMP(
        "Expand Flash Lamp", (short) 0xafc5
    ), PADDING(
        "Padding", (short) 0xea1c
    ), UNKNOWN(
        "Unknown", (short) 0xffff
    );

    private ExifAndTiffTag(
        String name,
        short value
    ) {
        this.name = name;
        this.value = value;
    }

    public static ExifAndTiffTag fromShort(short value) {
        ExifAndTiffTag exifTag = ExifAndTiffTag.tagMap.get(value);
        if (exifTag == null) { return UNKNOWN; }
        return exifTag;
    }

    private static final Map<Short, ExifAndTiffTag> tagMap = new HashMap<Short, ExifAndTiffTag>();

    static {
        for (ExifAndTiffTag exifTag : ExifAndTiffTag.values()) {
            ExifAndTiffTag.tagMap.put(exifTag.getValue(), exifTag);
        }
    }

    private final String name;
    private final short  value;

    public String getName() { return this.name; }

    public short getValue() { return this.value; }

}