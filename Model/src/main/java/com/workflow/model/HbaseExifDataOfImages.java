package com.workflow.model;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Generated;

import org.apache.avro.reflect.Nullable;

@HbaseTableName("image_exif_data_of_image")
public class HbaseExifDataOfImages extends HbaseData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String          imageId;
    @Column(hbaseName = "exif_tag", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_EXIF_TAG)
    protected long            exifTag;
    @Column(hbaseName = "exif_path", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteShortArray.class, fixedWidth = ModelConstants.FIXED_WIDTH_EXIF_PATH)
    protected short[]         exifPath;
    // Data
    @Nullable
    @Column(hbaseName = "exv_bytes", toByte = ToByteIdempotent.class, columnFamily = "exv", rowKeyNumber = 100)
    protected byte[]          exifValueAsByte;
    @Nullable
    @Column(hbaseName = "exv_ints", toByte = ToByteIntArray.class, columnFamily = "exv", rowKeyNumber = 100)
    protected int[]           exifValueAsInt;
    @Nullable
    @Column(hbaseName = "exv_shorts", toByte = ToByteShortArray.class, columnFamily = "exv", rowKeyNumber = 100)
    protected short[]         exifValueAsShort;
    @Nullable
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "imd", rowKeyNumber = 101)
    protected String          thumbName        = "";
    @Column(hbaseName = "creation_date", toByte = ToByteString.class, columnFamily = "imd", rowKeyNumber = 102)
    protected String          creationDate     = "";
    @Column(hbaseName = "width", toByte = ToByteLong.class, columnFamily = "sz", rowKeyNumber = 103)
    protected long            width;
    @Column(hbaseName = "height", toByte = ToByteLong.class, columnFamily = "sz", rowKeyNumber = 104)
    protected long            height;

    @Generated("SparkTools")
    private HbaseExifDataOfImages(Builder builder) {
        super(builder.dataId,
            System.currentTimeMillis());
        this.imageId = builder.imageId;
        this.exifTag = builder.exifTag;
        this.exifPath = builder.exifPath;
        this.exifValueAsByte = builder.exifValueAsByte;
        this.exifValueAsInt = builder.exifValueAsInt;
        this.exifValueAsShort = builder.exifValueAsShort;
        this.thumbName = builder.thumbName;
        this.creationDate = builder.creationDate;
        this.width = builder.width;
        this.height = builder.height;
    }

    public HbaseExifDataOfImages() { super(null,
        0); }

    public long getExifTag() { return this.exifTag; }

    public void setExifTag(long exifTag) { this.exifTag = exifTag; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public long getWidth() { return this.width; }

    public void setWidth(long width) { this.width = width; }

    public long getHeight() { return this.height; }

    public void setHeight(long height) { this.height = height; }

    public String getThumbName() { return this.thumbName; }

    public void setThumbName(String thumbName) { this.thumbName = thumbName; }

    public byte[] getExifValueAsByte() { return this.exifValueAsByte; }

    public void setExifValueAsByte(byte[] exifValueAsByte) { this.exifValueAsByte = exifValueAsByte; }

    public int[] getExifValueAsInt() { return this.exifValueAsInt; }

    public void setExifValueAsInt(int[] exifValueAsInt) { this.exifValueAsInt = exifValueAsInt; }

    public short[] getExifValueAsShort() { return this.exifValueAsShort; }

    public void setExifValueAsShort(short[] exifValueAsShort) { this.exifValueAsShort = exifValueAsShort; }

    public String getCreationDate() { return this.creationDate; }

    public short[] getExifPath() { return this.exifPath; }

    public void setExifPath(short[] exifPath) { this.exifPath = exifPath; }

    public void setCreationDate(String creationDate) { this.creationDate = creationDate; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((this.creationDate == null) ? 0 : this.creationDate.hashCode());
        result = (prime * result) + Arrays.hashCode(this.exifPath);
        result = (prime * result) + (int) (this.exifTag ^ (this.exifTag >>> 32));
        result = (prime * result) + Arrays.hashCode(this.exifValueAsByte);
        result = (prime * result) + Arrays.hashCode(this.exifValueAsInt);
        result = (prime * result) + Arrays.hashCode(this.exifValueAsShort);
        result = (prime * result) + (int) (this.height ^ (this.height >>> 32));
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        result = (prime * result) + ((this.thumbName == null) ? 0 : this.thumbName.hashCode());
        result = (prime * result) + (int) (this.width ^ (this.width >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseExifDataOfImages other = (HbaseExifDataOfImages) obj;
        if (this.creationDate == null) {
            if (other.creationDate != null) { return false; }
        } else if (!this.creationDate.equals(other.creationDate)) { return false; }
        if (!Arrays.equals(this.exifPath, other.exifPath)) { return false; }
        if (this.exifTag != other.exifTag) { return false; }
        if (!Arrays.equals(this.exifValueAsByte, other.exifValueAsByte)) { return false; }
        if (!Arrays.equals(this.exifValueAsInt, other.exifValueAsInt)) { return false; }
        if (!Arrays.equals(this.exifValueAsShort, other.exifValueAsShort)) { return false; }
        if (this.height != other.height) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.thumbName == null) {
            if (other.thumbName != null) { return false; }
        } else if (!this.thumbName.equals(other.thumbName)) { return false; }
        if (this.width != other.width) { return false; }
        return true;
    }

    /**
     * Creates builder to build {@link HbaseExifDataOfImages}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseExifDataOfImages}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String  dataId;
        private String  imageId;
        private long    exifTag;
        private short[] exifPath;
        private byte[]  exifValueAsByte;
        private int[]   exifValueAsInt;
        private short[] exifValueAsShort;
        private String  thumbName;
        private String  creationDate;
        private long    width;
        private long    height;

        private Builder() {}

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
         * Builder method for exifTag parameter.
         *
         * @param exifTag
         *            field to set
         * @return builder
         */
        public Builder withExifTag(long exifTag) {
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
        public Builder withCreationDate(String creationDate) {
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
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseExifDataOfImages build() { return new HbaseExifDataOfImages(this); }
    }

}
