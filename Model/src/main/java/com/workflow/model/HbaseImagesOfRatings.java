package com.workflow.model;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Generated;

@HbaseTableName("images_ratings")
public class HbaseImagesOfRatings extends HbaseData {
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "ratings", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteInt.class, fixedWidth = ModelConstants.FIXED_WIDTH_RATINGS)
    protected int             ratings;

    @Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
    protected long            creationDate;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String          imageId;
    @Column(hbaseName = "version", isPartOfRowkey = true, rowKeyNumber = 3, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected short           version;

    @Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
    protected String          imageName        = "";
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
    protected String          thumbName        = "";
    @Column(hbaseName = "thumbnail", toByte = ToByteIdempotent.class, columnFamily = "thb", rowKeyNumber = 102)
    protected byte[]          thumbnail        = {};
    @Column(hbaseName = "path", rowKeyNumber = 103, toByte = ToByteString.class, columnFamily = "img")
    protected String          path             = "";
    @Column(hbaseName = "width", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            width;
    @Column(hbaseName = "height", rowKeyNumber = 105, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            height;
    @Column(hbaseName = "originalWidth", rowKeyNumber = 106, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalWidth;
    @Column(hbaseName = "originalHeight", rowKeyNumber = 107, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalHeight;
    @Column(hbaseName = "importDate", rowKeyNumber = 108, toByte = ToByteLong.class, columnFamily = "img")
    protected long            importDate;
    @Column(hbaseName = "orientation", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 109)
    protected long            orientation;

    @Generated("SparkTools")
    private HbaseImagesOfRatings(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.ratings = builder.ratings;
        this.creationDate = builder.creationDate;
        this.imageId = builder.imageId;
        this.version = builder.version;
        this.imageName = builder.imageName;
        this.thumbName = builder.thumbName;
        this.thumbnail = builder.thumbnail;
        this.path = builder.path;
        this.width = builder.width;
        this.height = builder.height;
        this.originalWidth = builder.originalWidth;
        this.originalHeight = builder.originalHeight;
        this.importDate = builder.importDate;
        this.orientation = builder.orientation;
    }

    public HbaseImagesOfRatings() { super(); }

    public HbaseImagesOfRatings(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public int getRatings() { return this.ratings; }

    public void setRatings(int ratings) { this.ratings = ratings; }

    public long getCreationDate() { return this.creationDate; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public short getVersion() { return this.version; }

    public void setVersion(short version) { this.version = version; }

    public String getImageName() { return this.imageName; }

    public void setImageName(String imageName) { this.imageName = imageName; }

    public String getThumbName() { return this.thumbName; }

    public void setThumbName(String thumbName) { this.thumbName = thumbName; }

    public byte[] getThumbnail() { return this.thumbnail; }

    public void setThumbnail(byte[] thumbnail) { this.thumbnail = thumbnail; }

    public String getPath() { return this.path; }

    public void setPath(String path) { this.path = path; }

    public long getWidth() { return this.width; }

    public void setWidth(long width) { this.width = width; }

    public long getHeight() { return this.height; }

    public void setHeight(long height) { this.height = height; }

    public long getOriginalWidth() { return this.originalWidth; }

    public void setOriginalWidth(long originalWidth) { this.originalWidth = originalWidth; }

    public long getOriginalHeight() { return this.originalHeight; }

    public void setOriginalHeight(long originalHeight) { this.originalHeight = originalHeight; }

    public long getImportDate() { return this.importDate; }

    public void setImportDate(long importDate) { this.importDate = importDate; }

    public long getOrientation() { return this.orientation; }

    public void setOrientation(long orientation) { this.orientation = orientation; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Arrays.hashCode(this.thumbnail);
        result = (prime * result) + Objects.hash(
            this.creationDate,
            this.height,
            this.imageId,
            this.imageName,
            this.importDate,
            this.orientation,
            this.originalHeight,
            this.originalWidth,
            this.path,
            this.ratings,
            this.thumbName,
            this.version,
            this.width);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImagesOfRatings other = (HbaseImagesOfRatings) obj;
        return (this.creationDate == other.creationDate) && (this.height == other.height)
            && Objects.equals(this.imageId, other.imageId) && Objects.equals(this.imageName, other.imageName)
            && (this.importDate == other.importDate) && (this.orientation == other.orientation)
            && (this.originalHeight == other.originalHeight) && (this.originalWidth == other.originalWidth)
            && Objects.equals(this.path, other.path) && (this.ratings == other.ratings)
            && Objects.equals(this.thumbName, other.thumbName) && Arrays.equals(this.thumbnail, other.thumbnail)
            && (this.version == other.version) && (this.width == other.width);
    }

    @Override
    public String toString() {
        return "HbaseImagesOfRatings [ratings=" + this.ratings + ", creationDate=" + this.creationDate + ", imageId="
            + this.imageId + ", version=" + this.version + ", imageName=" + this.imageName + ", thumbName="
            + this.thumbName + ", thumbnail=" + Arrays.toString(this.thumbnail) + ", path=" + this.path + ", width="
            + this.width + ", height=" + this.height + ", originalWidth=" + this.originalWidth + ", originalHeight="
            + this.originalHeight + ", importDate=" + this.importDate + ", orientation=" + this.orientation + "]";
    }

    /**
     * Creates builder to build {@link HbaseImagesOfRatings}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImagesOfRatings}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private int    ratings;
        private long   creationDate;
        private String imageId;
        private short  version;
        private String imageName;
        private String thumbName;
        private byte[] thumbnail;
        private String path;
        private long   width;
        private long   height;
        private long   originalWidth;
        private long   originalHeight;
        private long   importDate;
        private long   orientation;

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
         * Builder method for ratings parameter.
         *
         * @param ratings
         *            field to set
         * @return builder
         */
        public Builder withRatings(int ratings) {
            this.ratings = ratings;
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
         * Builder method for version parameter.
         *
         * @param version
         *            field to set
         * @return builder
         */
        public Builder withVersion(short version) {
            this.version = version;
            return this;
        }

        /**
         * Builder method for imageName parameter.
         *
         * @param imageName
         *            field to set
         * @return builder
         */
        public Builder withImageName(String imageName) {
            this.imageName = imageName;
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
         * Builder method for thumbnail parameter.
         *
         * @param thumbnail
         *            field to set
         * @return builder
         */
        public Builder withThumbnail(byte[] thumbnail) {
            this.thumbnail = thumbnail;
            return this;
        }

        /**
         * Builder method for path parameter.
         *
         * @param path
         *            field to set
         * @return builder
         */
        public Builder withPath(String path) {
            this.path = path;
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
         * Builder method for originalWidth parameter.
         *
         * @param originalWidth
         *            field to set
         * @return builder
         */
        public Builder withOriginalWidth(long originalWidth) {
            this.originalWidth = originalWidth;
            return this;
        }

        /**
         * Builder method for originalHeight parameter.
         *
         * @param originalHeight
         *            field to set
         * @return builder
         */
        public Builder withOriginalHeight(long originalHeight) {
            this.originalHeight = originalHeight;
            return this;
        }

        /**
         * Builder method for importDate parameter.
         *
         * @param importDate
         *            field to set
         * @return builder
         */
        public Builder withImportDate(long importDate) {
            this.importDate = importDate;
            return this;
        }

        /**
         * Builder method for orientation parameter.
         *
         * @param orientation
         *            field to set
         * @return builder
         */
        public Builder withOrientation(long orientation) {
            this.orientation = orientation;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImagesOfRatings build() { return new HbaseImagesOfRatings(this); }
    }

}
