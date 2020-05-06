package com.workflow.model;

import java.util.Arrays;

import javax.annotation.Generated;

@HbaseTableName("image_thumbnail")
public class HbaseImageThumbnail extends HbaseData {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
    protected long            creationDate;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String          imageId;
    @Column(hbaseName = "version", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected short           version;

    // Data

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
    @Column(hbaseName = "originalWidth", rowKeyNumber = 105, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalWidth;
    @Column(hbaseName = "originalHeight", rowKeyNumber = 106, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalHeight;
    @Column(hbaseName = "importDate", rowKeyNumber = 107, toByte = ToByteLong.class, columnFamily = "img")
    protected long            importDate;

    @Generated("SparkTools")
    private HbaseImageThumbnail(Builder builder) {
        super(builder.dataId,
            System.currentTimeMillis());
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
    }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public long getCreationDate() { return this.creationDate; }

    public short getVersion() { return this.version; }

    public void setVersion(short version) { this.version = version; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public byte[] getThumbnail() { return this.thumbnail; }

    public void setThumbnail(byte[] thumbnail) { this.thumbnail = thumbnail; }

    public String getPath() { return this.path; }

    public void setPath(String path) { this.path = path; }

    public String getImageName() { return this.imageName; }

    public void setImageName(String imageName) { this.imageName = imageName; }

    public String getThumbName() { return this.thumbName; }

    public void setThumbName(String thumbName) { this.thumbName = thumbName; }

    public long getWidth() { return this.width; }

    public void setWidth(long width) { this.width = width; }

    public long getHeight() { return this.height; }

    public void setHeight(long height) { this.height = height; }

    public long getOriginalWidth() { return this.originalWidth; }

    public long getOriginalHeight() { return this.originalHeight; }

    public long getImportDate() { return this.importDate; }

    public HbaseImageThumbnail() { super(null,
        0); }

    @Override
    public Object clone() throws CloneNotSupportedException {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw e;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + (int) (this.creationDate ^ (this.creationDate >>> 32));
        result = (prime * result) + (int) (this.height ^ (this.height >>> 32));
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        result = (prime * result) + ((this.imageName == null) ? 0 : this.imageName.hashCode());
        result = (prime * result) + (int) (this.originalHeight ^ (this.originalHeight >>> 32));
        result = (prime * result) + (int) (this.originalWidth ^ (this.originalWidth >>> 32));
        result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
        result = (prime * result) + ((this.thumbName == null) ? 0 : this.thumbName.hashCode());
        result = (prime * result) + Arrays.hashCode(this.thumbnail);
        result = (prime * result) + this.version;
        result = (prime * result) + (int) (this.width ^ (this.width >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImageThumbnail other = (HbaseImageThumbnail) obj;
        if (this.creationDate != other.creationDate) { return false; }
        if (this.height != other.height) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.imageName == null) {
            if (other.imageName != null) { return false; }
        } else if (!this.imageName.equals(other.imageName)) { return false; }
        if (this.originalHeight != other.originalHeight) { return false; }
        if (this.originalWidth != other.originalWidth) { return false; }
        if (this.path == null) {
            if (other.path != null) { return false; }
        } else if (!this.path.equals(other.path)) { return false; }
        if (this.thumbName == null) {
            if (other.thumbName != null) { return false; }
        } else if (!this.thumbName.equals(other.thumbName)) { return false; }
        if (!Arrays.equals(this.thumbnail, other.thumbnail)) { return false; }
        if (this.version != other.version) { return false; }
        if (this.width != other.width) { return false; }
        return true;
    }

    @Override
    public String toString() {
        return "HbaseImageThumbnail [creationDate=" + this.creationDate + ", imageId=" + this.imageId + ", version="
            + this.version + ", imageName=" + this.imageName + ", thumbName=" + this.thumbName + ", thumbnail="
            + (this.thumbnail != null ? this.thumbnail.length : "<unset>") + ", path=" + this.path + ", width="
            + this.width + ", height=" + this.height + ", originalWidth=" + this.originalWidth + ", originalHeight="
            + this.originalHeight + "]";
    }

    /**
     * Creates builder to build {@link HbaseImageThumbnail}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageThumbnail}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String dataId;
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
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImageThumbnail build() { return new HbaseImageThumbnail(this); }
    }

}
