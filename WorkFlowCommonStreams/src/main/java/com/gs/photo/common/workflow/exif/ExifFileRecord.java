package com.gs.photo.common.workflow.exif;

import javax.annotation.Generated;

public class ExifFileRecord implements Comparable<ExifFileRecord> {
    protected String    tagHex;
    protected FieldType type;
    protected int       tagDec;
    protected String    ifd;
    protected String    key;
    protected String    description;

    @Generated("SparkTools")
    private ExifFileRecord(Builder builder) {
        this.tagHex = builder.tagHex;
        this.type = builder.type;
        this.tagDec = builder.tagDec;
        this.ifd = builder.ifd;
        this.key = builder.key;
        this.description = builder.description;
    }

    @Override
    public int compareTo(ExifFileRecord o) {
        int retValue = this.ifd.compareTo(o.ifd);

        if (retValue != 0) { return retValue; }
        return Integer.compare(this.tagDec, o.tagDec);
    }

    public String getTagHex() { return this.tagHex; }

    public void setTagHex(String tagHex) { this.tagHex = tagHex; }

    public int getTagDec() { return this.tagDec; }

    public void setTagDec(int tagDec) { this.tagDec = tagDec; }

    public String getIfd() { return this.ifd; }

    public void setIfd(String ifd) { this.ifd = ifd; }

    public String getKey() { return this.key; }

    public void setKey(String key) { this.key = key; }

    public String getDescription() { return this.description; }

    public void setDescription(String description) { this.description = description; }

    public FieldType getType() { return this.type; }

    public void setType(FieldType type) { this.type = type; }

    /**
     * Creates builder to build {@link ExifFileRecord}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ExifFileRecord}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String    tagHex;
        private FieldType type;
        private int       tagDec;
        private String    ifd;
        private String    key;
        private String    description;

        private Builder() {}

        /**
         * Builder method for tagHex parameter.
         *
         * @param tagHex
         *            field to set
         * @return builder
         */
        public Builder withTagHex(String tagHex) {
            this.tagHex = tagHex;
            return this;
        }

        /**
         * Builder method for type parameter.
         *
         * @param type
         *            field to set
         * @return builder
         */
        public Builder withType(FieldType type) {
            this.type = type;
            return this;
        }

        /**
         * Builder method for tagDec parameter.
         *
         * @param tagDec
         *            field to set
         * @return builder
         */
        public Builder withTagDec(int tagDec) {
            this.tagDec = tagDec;
            return this;
        }

        /**
         * Builder method for ifd parameter.
         *
         * @param ifd
         *            field to set
         * @return builder
         */
        public Builder withIfd(String ifd) {
            this.ifd = ifd;
            return this;
        }

        /**
         * Builder method for key parameter.
         *
         * @param key
         *            field to set
         * @return builder
         */
        public Builder withKey(String key) {
            this.key = key;
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
        public ExifFileRecord build() { return new ExifFileRecord(this); }
    }

}
