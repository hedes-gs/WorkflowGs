package com.workflow.model.dtos;

import java.time.OffsetDateTime;

import jakarta.xml.bind.annotation.XmlType;

@XmlType
public class ImageKeyDto {
    protected short          salt;
    protected OffsetDateTime creationDate;
    protected int            version;
    protected String         imageId;

    private ImageKeyDto(Builder builder) {
        this.salt = builder.salt;
        this.creationDate = builder.creationDate;
        this.version = builder.version;
        this.imageId = builder.imageId;
    }

    public ImageKeyDto(String content) {
        // creationDate,2020-05-18T15:38:31.395Z,version,0,imageId,string
        String[] split = content.split(",");
        // this.creationDate = OffsetDateTime
    }

    public int getVersion() { return this.version; }

    public void setCreationDate(OffsetDateTime creationDate) { this.creationDate = creationDate; }

    public void setVersion(int version) { this.version = version; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public String getImageId() { return this.imageId; }

    public ImageKeyDto() {}

    public OffsetDateTime getCreationDate() { return this.creationDate; }

    public short getSalt() { return this.salt; }

    public void setSalt(short salt) { this.salt = salt; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((this.creationDate == null) ? 0 : this.creationDate.hashCode());
        result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
        result = (prime * result) + this.version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        ImageKeyDto other = (ImageKeyDto) obj;
        if (this.creationDate == null) {
            if (other.creationDate != null) { return false; }
        } else if (!this.creationDate.equals(other.creationDate)) { return false; }
        if (this.imageId == null) {
            if (other.imageId != null) { return false; }
        } else if (!this.imageId.equals(other.imageId)) { return false; }
        if (this.version != other.version) { return false; }
        return true;
    }

    /**
     * Creates builder to build {@link ImageKeyDto}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImageKeyDto}.
     */
    public static final class Builder {
        private short          salt;
        private OffsetDateTime creationDate;
        private int            version;
        private String         imageId;

        private Builder() {}

        /**
         * Builder method for salt parameter.
         *
         * @param salt
         *            field to set
         * @return builder
         */
        public Builder withSalt(short salt) {
            this.salt = salt;
            return this;
        }

        /**
         * Builder method for creationDate parameter.
         *
         * @param creationDate
         *            field to set
         * @return builder
         */
        public Builder withCreationDate(OffsetDateTime creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        /**
         * Builder method for version parameter.
         *
         * @param version
         *            field to set
         * @return builder
         */
        public Builder withVersion(int version) {
            this.version = version;
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
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImageKeyDto build() { return new ImageKeyDto(this); }
    }

}