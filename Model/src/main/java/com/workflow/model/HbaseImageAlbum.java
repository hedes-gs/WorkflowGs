package com.workflow.model;

@HbaseTableName("image_album")
public class HbaseImageAlbum extends HbaseData {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "album", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_ALBUM_NAME)
    protected String          album;

    @Column(hbaseName = "description", isPartOfRowkey = false, rowKeyNumber = 100, toByte = ToByteString.class)
    protected String          description;

    private HbaseImageAlbum(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.album = builder.album;
        this.description = builder.description;
    }

    public String getAlbum() { return this.album; }

    public void setAlbum(String album) { this.album = album; }

    public String getDescription() { return this.description; }

    public void setDescription(String description) { this.description = description; }

    public HbaseImageAlbum() { super(null,
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
        result = (prime * result) + ((this.album == null) ? 0 : this.album.hashCode());
        result = (prime * result) + ((this.description == null) ? 0 : this.description.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImageAlbum other = (HbaseImageAlbum) obj;
        if (this.album == null) {
            if (other.album != null) { return false; }
        } else if (!this.album.equals(other.album)) { return false; }
        if (this.description == null) {
            if (other.description != null) { return false; }
        } else if (!this.description.equals(other.description)) { return false; }
        return true;
    }

    @Override
    public String toString() {
        return "HbaseImageAlbum [album=" + this.album + ", description=" + this.description + "]";
    }

    /**
     * Creates builder to build {@link HbaseImageAlbum}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageAlbum}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String album;
        private String description;

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
         * Builder method for album parameter.
         * 
         * @param album
         *            field to set
         * @return builder
         */
        public Builder withAlbum(String album) {
            this.album = album;
            return this;
        }

        /**
         * Builder method for description parameter.
         * 
         * @param description
         *            field to set
         * @return builder
         */
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseImageAlbum build() { return new HbaseImageAlbum(this); }
    }

}
