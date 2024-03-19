package com.workflow.model;

import org.apache.avro.reflect.Nullable;

@HbaseTableName(value = "album")
public class HbaseAlbum extends HbaseData {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "album_name", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_ALBUM_NAME)
    protected String          albumName;

    @Column(hbaseName = "description", rowKeyNumber = 100, toByte = ToByteString.class, columnFamily = "meta")
    @Nullable
    protected String          description;

    @Column(hbaseName = "nbOfElements", rowKeyNumber = 101, toByte = ToByteLong.class, columnFamily = "infos")
    protected long            nbOfElements;

    private HbaseAlbum(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.albumName = builder.albumName;
        this.description = builder.description;
        this.nbOfElements = builder.nbOfElements;
    }

    public HbaseAlbum() { super(); }

    public HbaseAlbum(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public String getAlbumName() { return this.albumName; }

    public void setAlbumName(String albumName) { this.albumName = albumName; }

    public String getDescription() { return this.description; }

    public void setDescription(String description) { this.description = description; }

    public long getNbOfElements() { return this.nbOfElements; }

    public void setNbOfElements(long nbOfElements) { this.nbOfElements = nbOfElements; }

    /**
     * Creates builder to build {@link HbaseAlbum}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseAlbum}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String albumName;
        private String description;
        private long   nbOfElements;

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
         * Builder method for albumName parameter.
         * 
         * @param albumName
         *            field to set
         * @return builder
         */
        public Builder withAlbumName(String albumName) {
            this.albumName = albumName;
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
         * Builder method for nbOfElements parameter.
         * 
         * @param nbOfElements
         *            field to set
         * @return builder
         */
        public Builder withNbOfElements(long nbOfElements) {
            this.nbOfElements = nbOfElements;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseAlbum build() { return new HbaseAlbum(this); }
    }

}
