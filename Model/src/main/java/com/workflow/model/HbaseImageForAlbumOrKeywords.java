package com.workflow.model;

import java.io.Serializable;

public class HbaseImageForAlbumOrKeywords extends HbaseData implements Serializable {
    private static final long serialVersionUID = 1L;

    protected long            creationDate;
    protected String          imageId;
    protected short           version;
    protected byte[]          tumbnail;

    private HbaseImageForAlbumOrKeywords(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.creationDate = builder.creationDate;
        this.imageId = builder.imageId;
        this.version = builder.version;
        this.tumbnail = builder.tumbnail;
    }

    public long getCreationDate() { return this.creationDate; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public short getVersion() { return this.version; }

    public void setVersion(short version) { this.version = version; }

    public byte[] getTumbnail() { return this.tumbnail; }

    public void setTumbnail(byte[] tumbnail) { this.tumbnail = tumbnail; }

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
        HbaseImageForAlbumOrKeywords other = (HbaseImageForAlbumOrKeywords) obj;
        if (this.creationDate != other.creationDate) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.version != other.version) { return false; }
        return true;
    }

    public HbaseImageForAlbumOrKeywords() { super(); }

    public HbaseImageForAlbumOrKeywords(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    /**
     * Creates builder to build {@link HbaseImageForAlbumOrKeywords}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageForAlbumOrKeywords}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private long   creationDate;
        private String imageId;
        private short  version;
        private byte[] tumbnail;

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
         * Builder method for tumbnail parameter.
         * 
         * @param tumbnail
         *            field to set
         * @return builder
         */
        public Builder withTumbnail(byte[] tumbnail) {
            this.tumbnail = tumbnail;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseImageForAlbumOrKeywords build() { return new HbaseImageForAlbumOrKeywords(this); }
    }

}
