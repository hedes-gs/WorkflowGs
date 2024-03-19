package com.gs.photos.workflow.extimginfo.metadata.tiff;

import java.util.HashMap;
import java.util.Map;

import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.StringUtils;

public enum TiffTag implements Tag {
    DATE_TIME_ORIGINAL(
        "date time original", (short) 0x9003, Attribute.PRIVATE
    ), CREATE_DATE_TIME(
        "create date time ", (short) 0x9004, Attribute.PRIVATE
    ), OFFSET_TIME(
        "offset date time ", (short) 0x9010, Attribute.PRIVATE
    ), OFFSET_TIME_ORIGNAL(
        "offset time original", (short) 0x9011, Attribute.PRIVATE
    ), OFFSET_TIME_DIGITIZED(
        "offset time digitized", (short) 0x9012, Attribute.PRIVATE
    ), FOCAL_LENGTH(
        "focal length", (short) 0x920A, Attribute.PRIVATE
    ), FOCAL_LENGTH_IN_35_MM(
        "focal length in 35mm", (short) 0xA405, Attribute.PRIVATE
    ), OWNER_NAME(
        "Owner name", (short) 0xA430, Attribute.PRIVATE
    ), LENS_MODEL(
        "Lens model	", (short) 0xA434, Attribute.PRIVATE
    ), THUMB_JPEG(
        "thumb jpg", (short) 0xfffe, Attribute.PRIVATE
    ),
    // Definition includes all baseline and extended tags.
    NEW_SUBFILE_TYPE(
        "New Subfile Type", (short) 0x00FE, Attribute.BASELINE
    ),

    SUBFILE_TYPE(
        "Subfile Type", (short) 0x00FF, Attribute.BASELINE
    ),

    IMAGE_WIDTH(
        "Image Width", (short) 0x0100, Attribute.BASELINE
    ),

    IMAGE_LENGTH(
        "Image Length", (short) 0x0101, Attribute.BASELINE
    ),

    BITS_PER_SAMPLE(
        "Bits Per Sample", (short) 0x0102, Attribute.BASELINE
    ),

    COMPRESSION(
        "Compression", (short) 0x0103, Attribute.BASELINE
    ),

    PHOTOMETRIC_INTERPRETATION(
        "Photometric Interpretation", (short) 0x0106, Attribute.BASELINE
    ),

    THRESHOLDING(
        "Thresholding", (short) 0x0107, Attribute.BASELINE
    ),

    CELL_WIDTH(
        "Cell Width", (short) 0x0108, Attribute.BASELINE
    ),

    CELL_LENGTH(
        "Cell Length", (short) 0x0109, Attribute.BASELINE
    ),

    FILL_ORDER(
        "Fill Order", (short) 0x010A, Attribute.BASELINE
    ),

    DOCUMENT_NAME(
        "Document Name", (short) 0x010D, Attribute.EXTENDED
    ),

    IMAGE_DESCRIPTION(
        "Image Description", (short) 0x010E, Attribute.BASELINE
    ),

    MAKE(
        "Make", (short) 0x010F, Attribute.BASELINE
    ),

    MODEL(
        "Model", (short) 0x0110, Attribute.BASELINE
    ),

    STRIP_OFFSETS(
        "Strip Offsets", (short) 0x0111, Attribute.BASELINE
    ),

    ORIENTATION(
        "Orientation", (short) 0x0112, Attribute.BASELINE
    ),

    SAMPLES_PER_PIXEL(
        "Samples Per Pixel", (short) 0x0115, Attribute.BASELINE
    ),

    ROWS_PER_STRIP(
        "Rows Per Strip", (short) 0x0116, Attribute.BASELINE
    ),

    STRIP_BYTE_COUNTS(
        "Strip Byte Counts", (short) 0x0117, Attribute.BASELINE
    ),

    MIN_SAMPLE_VALUE(
        "Min Sample Value", (short) 0x0118, Attribute.BASELINE
    ),

    MAX_SAMPLE_VALUE(
        "Max Sample Value", (short) 0x0119, Attribute.BASELINE
    ),

    X_RESOLUTION(
        "XResolution", (short) 0x011A, Attribute.BASELINE
    ),

    Y_RESOLUTION(
        "YResolution", (short) 0x011B, Attribute.BASELINE
    ),

    PLANAR_CONFIGURATTION(
        "Planar Configuration", (short) 0x011C, Attribute.BASELINE
    ),

    PAGE_NAME(
        "Page Name", (short) 0x011D, Attribute.EXTENDED
    ), X_POSITION(
        "XPosition", (short) 0x011E, Attribute.EXTENDED
    ), Y_POSITION(
        "YPosition", (short) 0x011F, Attribute.EXTENDED
    ),

    FREE_OFFSETS(
        "Free Offsets", (short) 0x0120, Attribute.BASELINE
    ),

    FREE_BYTE_COUNTS(
        "Free Byte Counts", (short) 0x0121, Attribute.BASELINE
    ),

    /**
     * The precision of the information contained in the GrayResponseCurve. Because
     * optical density is specified in terms of fractional numbers, this field is
     * necessary to interpret the stored integer information. For example, if
     * GrayScaleResponseUnits is set to 4 (ten-thousandths of a unit), and a
     * GrayScaleResponseCurve number for gray level 4 is 3455, then the resulting
     * actual value is 0.3455.
     */
    GRAY_RESPONSE_UNIT(
        "Gray Response Unit", (short) 0x0122, Attribute.BASELINE
    ),

    GRAY_RESPONSE_CURVE(
        "Gray Response Curve", (short) 0x0123, Attribute.BASELINE
    ),

    T4_OPTIONS(
        "T4 Options", (short) 0x0124, Attribute.EXTENDED
    ),

    T6_OPTIONS(
        "T6 Options", (short) 0x0125, Attribute.EXTENDED
    ),

    RESOLUTION_UNIT(
        "Resolution Unit", (short) 0x0128, Attribute.BASELINE
    ),

    PAGE_NUMBER(
        "Page Number", (short) 0x0129, Attribute.EXTENDED
    ),

    TRANSFER_FUNCTION(
        "Transfer Function", (short) 0x012D, Attribute.EXTENDED
    ),

    SOFTWARE(
        "Software", (short) 0x0131, Attribute.BASELINE
    ),

    DATETIME(
        "DateTime", (short) 0x0132, Attribute.BASELINE
    ),

    ARTIST(
        "Artist", (short) 0x013B, Attribute.BASELINE
    ),

    HOST_COMPUTER(
        "Host Computer", (short) 0x013C, Attribute.BASELINE
    ),

    PREDICTOR(
        "Predictor", (short) 0x013D, Attribute.EXTENDED
    ),

    WHITE_POINT(
        "White Point", (short) 0x013E, Attribute.EXTENDED
    ), PRIMARY_CHROMATICITIES(
        "PrimaryChromaticities", (short) 0x013F, Attribute.EXTENDED
    ),

    COLORMAP(
        "ColorMap", (short) 0x0140, Attribute.BASELINE
    ),

    HALTONE_HINTS(
        "Halftone Hints", (short) 0x0141, Attribute.EXTENDED
    ),

    TILE_WIDTH(
        "Tile Width", (short) 0x0142, Attribute.EXTENDED
    ),

    TILE_LENGTH(
        "Tile Length", (short) 0x0143, Attribute.EXTENDED
    ),

    TILE_OFFSETS(
        "Tile Offsets", (short) 0x0144, Attribute.EXTENDED
    ),

    TILE_BYTE_COUNTS(
        "Tile Byte Counts", (short) 0x0145, Attribute.EXTENDED
    ),

    BAD_FAX_LINES(
        "Bad Fax Lines", (short) 0x0146, Attribute.EXTENDED
    ), CLEAN_FAX_DATA(
        "Clean Fax Data", (short) 0x0147, Attribute.EXTENDED
    ), CONSECUTIVE_BAD_FAX_LINES(
        "ConsecutiveBadFaxLines", (short) 0x0148, Attribute.EXTENDED
    ),

    SUB_IFDS(
        "Sub IFDs", (short) 0x014A, Attribute.EXTENDED
    ), EXIF_PRIVATE_TAGS(
        "Sony/Adobe DNG/Pentax tags", (short) 0xc634, Attribute.PRIVATE
    ),

    INK_SET(
        "Ink Set", (short) 0x014C, Attribute.EXTENDED
    ),

    INK_NAMES(
        "Ink Names", (short) 0x014D, Attribute.EXTENDED
    ), NUMBER_OF_INKS(
        "Number Of Inks", (short) 0x014E, Attribute.EXTENDED
    ), DOT_RANGE(
        "Dot Range", (short) 0x0150, Attribute.EXTENDED
    ), TARGET_PRINTER(
        "Target Printer", (short) 0x0151, Attribute.EXTENDED
    ),

    EXTRA_SAMPLES(
        "Extra Samples", (short) 0x0152, Attribute.BASELINE
    ),

    SAMPLE_FORMAT(
        "Sample Format", (short) 0x0153, Attribute.EXTENDED
    ),

    S_MIN_SAMPLE_VALUE(
        "S Min Sample Value", (short) 0x0154, Attribute.EXTENDED
    ), S_MAX_SAMPLE_VALUE(
        "S Max Sample Value", (short) 0x0155, Attribute.EXTENDED
    ), TRANSFER_RANGE(
        "Transfer Range", (short) 0x0156, Attribute.EXTENDED
    ), CLIP_PATH(
        "Clip Path", (short) 0x0157, Attribute.EXTENDED
    ), X_CLIP_PATH_UNITS(
        "X Clip Path Units", (short) 0x0158, Attribute.EXTENDED
    ), Y_CLIP_PATH_UNITS(
        "Y Clip Path Units", (short) 0x0159, Attribute.EXTENDED
    ),

    INDEXED(
        "Indexed", (short) 0x015A, Attribute.EXTENDED
    ),

    JPEG_TABLES(
        "JPEGTables - optional, for new-style JPEG compression", (short) 0x015B, Attribute.EXTENDED
    ),

    OPI_PROXY(
        "OPI Proxy", (short) 0x015F, Attribute.EXTENDED
    ),

    GLOBAL_PARAMETERS_IFD(
        "Global Parameters IFD", (short) 0x0190, Attribute.EXTENDED
    ),

    PROFILE_TYPE(
        "Profile Type", (short) 0x0191, Attribute.EXTENDED
    ),

    FAX_PROFILE(
        "Fax Profile", (short) 0x0192, Attribute.EXTENDED
    ),

    CODING_METHODS(
        "Coding Methods", (short) 0x0193, Attribute.EXTENDED
    ),

    VERSION_YEAR(
        "Version Year", (short) 0x0194, Attribute.EXTENDED
    ),

    MODE_NUMBER(
        "Mode Number", (short) 0x0195, Attribute.EXTENDED
    ),

    DECODE(
        "Decode", (short) 0x01B1, Attribute.EXTENDED
    ), DEFAULT_IMAGE_COLOR(
        "Default Image Color", (short) 0x01B2, Attribute.EXTENDED
    ),

    JPEG_PROC(
        "JPEG Proc", (short) 0x0200, Attribute.EXTENDED
    ),

    JPEG_INTERCHANGE_FORMAT(
        "JPEG Interchange Format/Jpeg IF Offset", (short) 0x0201, Attribute.EXTENDED
    ),

    JPEG_INTERCHANGE_FORMAT_LENGTH(
        "JPEG Interchange Format Length/Jpeg IF Byte Count", (short) 0x0202, Attribute.EXTENDED
    ),

    JPEG_RESTART_INTERVAL(
        "JPEG Restart Interval", (short) 0x0203, Attribute.EXTENDED
    ),

    JPEG_LOSSLESS_PREDICTORS(
        "JPEG Lossless Predictors", (short) 0x0205, Attribute.EXTENDED
    ),

    JPEG_POINT_TRANSFORMS(
        "JPEG Point Transforms", (short) 0x0206, Attribute.EXTENDED
    ),

    JPEG_Q_TABLES(
        "JPEG Q Tables", (short) 0x0207, Attribute.EXTENDED
    ),

    JPEG_DC_TABLES(
        "JPEG DC Tables", (short) 0x0208, Attribute.EXTENDED
    ),

    JPEG_AC_TABLES(
        "JPEG AC Tables", (short) 0x0209, Attribute.EXTENDED
    ),

    YCbCr_COEFFICIENTS(
        "YCbCr Coefficients", (short) 0x0211, Attribute.EXTENDED
    ),

    YCbCr_SUB_SAMPLING(
        "YCbCr SubSampling", (short) 0x0212, Attribute.EXTENDED
    ),

    YCbCr_POSITIONING(
        "YCbCr Positioning", (short) 0x0213, Attribute.EXTENDED
    ),

    REFERENCE_BLACK_WHITE(
        "Reference Black White", (short) 0x0214, Attribute.EXTENDED
    ),

    STRIP_ROW_COUNTS(
        "Strip Row Counts", (short) 0x022F, Attribute.EXTENDED
    ),

    XMP(
        "XMP", (short) 0x02BC, Attribute.EXTENDED
    ),

    RATING(
        "Rating", (short) 0x4746, Attribute.PRIVATE
    ), RATING_PERCENT(
        "Rating Percent", (short) 0x4749, Attribute.PRIVATE
    ),

    IMAGE_ID(
        "Image ID", (short) 0x800D, Attribute.EXTENDED
    ),

    MATTEING(
        "Matteing", (short) 0x80e3, Attribute.PRIVATE
    ),

    COPYRIGHT(
        "Copyright", (short) 0x8298, Attribute.BASELINE
    ),

    // (International Press Telecommunications Council) metadata.
    IPTC(
        "Rich Tiff IPTC", (short) 0x83BB, Attribute.PRIVATE
    ),

    IT8_SITE(
        "IT8 Site", (short) 0x84e0, Attribute.PRIVATE
    ), IT8_COLOR_SEQUENCE(
        "IT8 Color Sequence", (short) 0x84e1, Attribute.PRIVATE
    ), IT8_HEADER(
        "IT8 Header", (short) 0x84e2, Attribute.PRIVATE
    ), IT8_RASTER_PADDING(
        "IT8 Raster Padding", (short) 0x84e3, Attribute.PRIVATE
    ), IT8_BITS_PER_RUN_LENGTH(
        "IT8 Bits Per Run Length", (short) 0x84e4, Attribute.PRIVATE
    ), IT8_BITS_PER_EXTENDED_RUN_LENGTH(
        "IT8 Bits Per Extended Run Length", (short) 0x84e5, Attribute.PRIVATE
    ), IT8_COLOR_TABLE(
        "IT8 Color Table", (short) 0x84e6, Attribute.PRIVATE
    ), IT8_IMAGE_COLOR_INDICATOR(
        "IT8 Image Color Indicator", (short) 0x84e7, Attribute.PRIVATE
    ), IT8_BKG_COLOR_INDICATOR(
        "IT8 Bkg Color Indicator", (short) 0x84e8, Attribute.PRIVATE
    ), IT8_IMAGE_COLOR_VALUE(
        "IT8 Image Color Value", (short) 0x84e9, Attribute.PRIVATE
    ), IT8_BKG_COLOR_VALUE(
        "IT8 Bkg Color Value", (short) 0x84ea, Attribute.PRIVATE
    ), IT8_PIXEL_INTENSITY_RANGE(
        "IT8 Pixel Intensity Range", (short) 0x84eb, Attribute.PRIVATE
    ), IT8_TRANSPARENCY_INDICATOR(
        "IT8 Transparency Indicator", (short) 0x84ec, Attribute.PRIVATE
    ), IT8_COLOR_CHARACTERIZATION(
        "IT8 Color Characterization", (short) 0x84ed, Attribute.PRIVATE
    ), IT8_HC_USAGE(
        "IT8 HC Usage", (short) 0x84ee, Attribute.PRIVATE
    ),

    IPTC2(
        "Rich Tiff IPTC", (short) 0x8568, Attribute.PRIVATE
    ),

    FRAME_COUNT(
        "Frame Count", (short) 0x85b8, Attribute.PRIVATE
    ),

    // Photoshop image resources
    PHOTOSHOP(
        "Photoshop", (short) 0x8649, Attribute.PRIVATE
    ),

    // The following tag is for ExifSubIFD
    EXIF_SUB_IFD(
        "Exif Sub IFD", (short) 0x8769, Attribute.PRIVATE
    ),

    IMAGE_LAYER(
        "Image Layer", (short) 0x87ac, Attribute.EXTENDED
    ),

    ICC_PROFILE(
        "ICC Profile", (short) 0x8773, Attribute.PRIVATE
    ),

    // The following tag is for GPSSubIFD
    GPS_SUB_IFD(
        "GPS Sub IFD", (short) 0x8825, Attribute.PRIVATE
    ),

    /*
     * Photoshop-specific TIFF tag. Starts with a null-terminated character string
     * of "Adobe Photoshop Document Data Block"
     */
    IMAGE_SOURCE_DATA(
        "Image Source Data", (short) 0x935C, Attribute.PRIVATE
    ),

    WINDOWS_XP_TITLE(
        "WindowsXP Title", (short) 0x9c9b, Attribute.PRIVATE
    ),

    WINDOWS_XP_COMMENT(
        "WindowsXP Comment", (short) 0x9c9c, Attribute.PRIVATE
    ),

    WINDOWS_XP_AUTHOR(
        "WindowsXP Author", (short) 0x9c9d, Attribute.PRIVATE
    ),

    WINDOWS_XP_KEYWORDS(
        "WindowsXP Keywords", (short) 0x9c9e, Attribute.PRIVATE
    ),

    WINDOWS_XP_SUBJECT(
        "WindowsXP Subject", (short) 0x9c9f, Attribute.PRIVATE
    ),

    // Ranking unknown tag the least significant.
    UNKNOWN(
        "Unknown", (short) 0xffff, Attribute.UNKNOWN
    );

    public enum Attribute {
        BASELINE, EXTENDED, PRIVATE, UNKNOWN;

        @Override
        public String toString() { return StringUtils.capitalizeFully(this.name()); }
    }

    private static final Map<Short, TiffTag> tagMap = new HashMap<Short, TiffTag>();

    static {
        for (TiffTag tiffTag : TiffTag.values()) {
            TiffTag.tagMap.put(tiffTag.getValue(), tiffTag);
        }
    }

    public static Tag fromShort(short value) {
        TiffTag tiffTag = TiffTag.tagMap.get(value);
        if (tiffTag == null) { return UNKNOWN; }
        return tiffTag;
    }

    private final String    name;

    private final short     value;

    private final Attribute attribute;

    private TiffTag(
        String name,
        short value,
        Attribute attribute
    ) {
        this.name = name;
        this.value = value;
        this.attribute = attribute;
    }

    public Attribute getAttribute() { return this.attribute; }

    /**
     * Intended to be overridden by certain tags to provide meaningful string
     * representation of the field value such as compression, photo metric
     * interpretation etc.
     *
     * @param value
     *            field value to be mapped to a string
     * @return a string representation of the field value or empty string if no
     *         meaningful string representation exists.
     */
    @Override
    public String getName() { return this.name; }

    @Override
    public short getValue() { return this.value; }

    @Override
    public String toString() {
        if (this == UNKNOWN) { return this.name; }
        return this.name + " [Value: " + StringUtils.shortToHexStringMM(this.value) + "] (" + this.getAttribute() + ")";
    }

    @Override
    public FieldType getFieldType() { // TODO Auto-generated method stub
        return null;
    }

}