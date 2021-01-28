package com.workflow.model;

@HbaseTableName("image_thumbnail_key")
public class HbaseImageThumbnailKey extends HbaseData {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
    protected long            creationDate;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String          imageId;
    @Column(hbaseName = "version", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected short           version;

    private HbaseImageThumbnailKey(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.creationDate = builder.creationDate;
        this.imageId = builder.imageId;
        this.version = builder.version;
    }

    // Data
    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public long getCreationDate() { return this.creationDate; }

    public short getVersion() { return this.version; }

    public void setVersion(short version) { this.version = version; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public HbaseImageThumbnailKey() { super(null,
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
        int result = super.hashCode();
        result = (prime * result) + (int) (this.creationDate ^ (this.creationDate >>> 32));
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        result = (prime * result) + this.version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImageThumbnailKey other = (HbaseImageThumbnailKey) obj;
        if (this.creationDate != other.creationDate) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.version != other.version) { return false; }
        return true;
    }

    @Override
    public String toString() {
        return "HbaseImageThumbnailKey [creationDate=" + this.creationDate + ", imageId=" + this.imageId + ", version="
            + this.version + "]";
    }

    /**
     * Creates builder to build {@link HbaseImageThumbnailKey}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageThumbnailKey}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private long   creationDate;
        private String imageId;
        private short  version;

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
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseImageThumbnailKey build() { return new HbaseImageThumbnailKey(this); }
    }

}
