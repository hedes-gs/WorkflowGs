package com.workflow.model;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.avro.reflect.Nullable;

@HbaseTableName("image_exif")
public class HbaseExifData extends HbaseData implements Serializable, Cloneable {

    private static final long    serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "region_salt", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected short              regionSalt;
    @Column(hbaseName = "exif_tag", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_EXIF_TAG)
    protected short              exifTag;
    @Column(hbaseName = "exif_path", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteShortArray.class, fixedWidth = ModelConstants.FIXED_WIDTH_EXIF_PATH)
    protected short[]            exifPath;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 3, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String             imageId;

    // Data

    @Nullable
    @Column(hbaseName = "exv_bytes", toByte = ToByteIdempotent.class, columnFamily = "img", rowKeyNumber = 100)
    protected byte[]             exifValueAsByte;
    @Nullable
    @Column(hbaseName = "exv_ints", toByte = ToByteIntArray.class, columnFamily = "img", rowKeyNumber = 101)
    protected int[]              exifValueAsInt;
    @Nullable
    @Column(hbaseName = "exv_shorts", toByte = ToByteShortArray.class, columnFamily = "img", rowKeyNumber = 102)
    protected short[]            exifValueAsShort;
    @Nullable
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 103)
    protected String             thumbName        = "";
    @Column(hbaseName = "creation_date", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 104)
    protected long               creationDate;
    @Column(hbaseName = "width", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 105)
    protected long               width;
    @Column(hbaseName = "height", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 106)
    protected long               height;
    @Nullable
    @Column(hbaseName = "thumbnail", toByte = ToByteObject.class, columnFamily = "thb", rowKeyNumber = 107)
    protected SizeAndJpegContent thumbnail;

    private HbaseExifData(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.regionSalt = builder.regionSalt;
        this.exifTag = builder.exifTag;
        this.exifPath = builder.exifPath;
        this.imageId = builder.imageId;
        this.exifValueAsByte = builder.exifValueAsByte;
        this.exifValueAsInt = builder.exifValueAsInt;
        this.exifValueAsShort = builder.exifValueAsShort;
        this.thumbName = builder.thumbName;
        this.creationDate = builder.creationDate;
        this.width = builder.width;
        this.height = builder.height;
        this.thumbnail = builder.thumbnail;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw e;
        }
    }

    public short[] getExifPath() { return this.exifPath; }

    public HbaseExifData() { super(null,
        0); }

    public short getExifTag() { return this.exifTag; }

    public void setExifTag(short exifTag) { this.exifTag = exifTag; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public long getWidth() { return this.width; }

    public void setWidth(long width) { this.width = width; }

    public long getHeight() { return this.height; }

    public void setHeight(long height) { this.height = height; }

    public String getThumbName() { return this.thumbName; }

    public void setThumbName(String thumbName) { this.thumbName = thumbName; }

    public byte[] getExifValueAsByte() { return this.exifValueAsByte; }

    public int[] getExifValueAsInt() { return this.exifValueAsInt; }

    public short[] getExifValueAsShort() { return this.exifValueAsShort; }

    public long getCreationDate() { return this.creationDate; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public SizeAndJpegContent getThumbnail() { return this.thumbnail; }

    public void setThumbnail(SizeAndJpegContent thumbnail) { this.thumbnail = thumbnail; }

    public short getRegionSalt() { return this.regionSalt; }

    public void setRegionSalt(short regionSalt) { this.regionSalt = regionSalt; }

    public void setExifPath(short[] exifPath) { this.exifPath = exifPath; }

    public void setExifValueAsByte(byte[] exifValueAsByte) { this.exifValueAsByte = exifValueAsByte; }

    public void setExifValueAsInt(int[] exifValueAsInt) { this.exifValueAsInt = exifValueAsInt; }

    public void setExifValueAsShort(short[] exifValueAsShort) { this.exifValueAsShort = exifValueAsShort; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Arrays.hashCode(this.exifPath);
        result = (prime * result) + (this.exifTag ^ (this.exifTag >>> 32));
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseExifData other = (HbaseExifData) obj;
        if (!Arrays.equals(this.exifPath, other.exifPath)) { return false; }
        if (this.exifTag != other.exifTag) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        return true;
    }

    @Override
    public String toString() {
        return "HbaseExifData [exifTag=" + this.exifTag + ", imageId=" + this.imageId + ", exifValueAsByte="
            + Arrays.toString(this.exifValueAsByte) + ", exifValueAsInt=" + Arrays.toString(this.exifValueAsInt)
            + ", exifValueAsShort=" + Arrays.toString(this.exifValueAsShort) + ", thumbName=" + this.thumbName
            + ", creationDate=" + this.creationDate + ", width=" + this.width + ", height=" + this.height + "]";
    }

    /**
     * Creates builder to build {@link HbaseExifData}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseExifData}.
     */
    public static final class Builder {
        private long               dataCreationDate;
        private String             dataId;
        private short              regionSalt;
        private short              exifTag;
        private short[]            exifPath;
        private String             imageId;
        private byte[]             exifValueAsByte;
        private int[]              exifValueAsInt;
        private short[]            exifValueAsShort;
        private String             thumbName;
        private long               creationDate;
        private long               width;
        private long               height;
        private SizeAndJpegContent thumbnail;

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         *
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         *
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for regionSalt parameter.
         *
         * @param regionSalt
         *            field to set
         * @return builder
         */
        public Builder withRegionSalt(short regionSalt) {
            this.regionSalt = regionSalt;
            return this;
        }

        /**
         * Builder method for exifTag parameter.
         *
         * @param exifTag
         *            field to set
         * @return builder
         */
        public Builder withExifTag(short exifTag) {
            this.exifTag = exifTag;
            return this;
        }

        /**
         * Builder method for exifPath parameter.
         *
         * @param exifPath
         *            field to set
         * @return builder
         */
        public Builder withExifPath(short[] exifPath) {
            this.exifPath = exifPath;
            return this;
        }

        /**
         * Builder method for imageId parameter.
         *
         * @param imageId
         *            field to set
         * @return builder
         */
        public Builder withImageId(String imageId) {
            this.imageId = imageId;
            return this;
        }

        /**
         * Builder method for exifValueAsByte parameter.
         *
         * @param exifValueAsByte
         *            field to set
         * @return builder
         */
        public Builder withExifValueAsByte(byte[] exifValueAsByte) {
            this.exifValueAsByte = exifValueAsByte;
            return this;
        }

        /**
         * Builder method for exifValueAsInt parameter.
         *
         * @param exifValueAsInt
         *            field to set
         * @return builder
         */
        public Builder withExifValueAsInt(int[] exifValueAsInt) {
            this.exifValueAsInt = exifValueAsInt;
            return this;
        }

        /**
         * Builder method for exifValueAsShort parameter.
         *
         * @param exifValueAsShort
         *            field to set
         * @return builder
         */
        public Builder withExifValueAsShort(short[] exifValueAsShort) {
            this.exifValueAsShort = exifValueAsShort;
            return this;
        }

        /**
         * Builder method for thumbName parameter.
         *
         * @param thumbName
         *            field to set
         * @return builder
         */
        public Builder withThumbName(String thumbName) {
            this.thumbName = thumbName;
            return this;
        }

        /**
         * Builder method for creationDate parameter.
         *
         * @param creationDate
         *            field to set
         * @return builder
         */
        public Builder withCreationDate(long creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        /**
         * Builder method for width parameter.
         *
         * @param width
         *            field to set
         * @return builder
         */
        public Builder withWidth(long width) {
            this.width = width;
            return this;
        }

        /**
         * Builder method for height parameter.
         *
         * @param height
         *            field to set
         * @return builder
         */
        public Builder withHeight(long height) {
            this.height = height;
            return this;
        }

        /**
         * Builder method for thumbnail parameter.
         *
         * @param thumbnail
         *            field to set
         * @return builder
         */
        public Builder withThumbnail(SizeAndJpegContent thumbnail) {
            this.thumbnail = thumbnail;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseExifData build() { return new HbaseExifData(this); }
    }

}
