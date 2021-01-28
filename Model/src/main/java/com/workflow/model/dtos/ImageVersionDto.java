package com.workflow.model.dtos;

import java.time.OffsetDateTime;

import javax.xml.bind.annotation.XmlType;

@XmlType
public class ImageVersionDto {
    protected String         imageId;
    protected String         imageName;
    protected OffsetDateTime creationDate;
    protected OffsetDateTime importDate;
    protected int            version;
    protected int            thumbnailWidth;
    protected int            thumbnailHeight;
    protected int            originalWidth;
    protected int            originalHeight;
    protected int            orientation;
    protected byte[]         jpegContent;

    private ImageVersionDto(Builder builder) {
        this.imageId = builder.imageId;
        this.imageName = builder.imageName;
        this.creationDate = builder.creationDate;
        this.importDate = builder.importDate;
        this.version = builder.version;
        this.thumbnailWidth = builder.thumbnailWidth;
        this.thumbnailHeight = builder.thumbnailHeight;
        this.originalWidth = builder.originalWidth;
        this.originalHeight = builder.originalHeight;
        this.orientation = builder.orientation;
        this.jpegContent = builder.jpegContent;
    }

    public ImageVersionDto() {}

    public int getOrientation() { return this.orientation; }

    public String getImageId() { return this.imageId; }

    public String getImageName() { return this.imageName; }

    public OffsetDateTime getCreationDate() { return this.creationDate; }

    public OffsetDateTime getImportDate() { return this.importDate; }

    public int getThumbnailWidth() { return this.thumbnailWidth; }

    public int getThumbnailHeight() { return this.thumbnailHeight; }

    public int getOriginalWidth() { return this.originalWidth; }

    public int getOriginalHeight() { return this.originalHeight; }

    public byte[] getJpegContent() { return this.jpegContent; }

    /**
     * Creates builder to build {@link ImageVersionDto}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImageVersionDto}.
     */
    public static final class Builder {
        private String         imageId;
        private String         imageName;
        private OffsetDateTime creationDate;
        private OffsetDateTime importDate;
        private int            version;
        private int            thumbnailWidth;
        private int            thumbnailHeight;
        private int            originalWidth;
        private int            originalHeight;
        private int            orientation;
        private byte[]         jpegContent;

        private Builder() {}

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
         * Builder method for importDate parameter.
         *
         * @param importDate
         *            field to set
         * @return builder
         */
        public Builder withImportDate(OffsetDateTime importDate) {
            this.importDate = importDate;
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
         * Builder method for thumbnailWidth parameter.
         *
         * @param thumbnailWidth
         *            field to set
         * @return builder
         */
        public Builder withThumbnailWidth(int thumbnailWidth) {
            this.thumbnailWidth = thumbnailWidth;
            return this;
        }

        /**
         * Builder method for thumbnailHeight parameter.
         *
         * @param thumbnailHeight
         *            field to set
         * @return builder
         */
        public Builder withThumbnailHeight(int thumbnailHeight) {
            this.thumbnailHeight = thumbnailHeight;
            return this;
        }

        /**
         * Builder method for originalWidth parameter.
         *
         * @param originalWidth
         *            field to set
         * @return builder
         */
        public Builder withOriginalWidth(int originalWidth) {
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
        public Builder withOriginalHeight(int originalHeight) {
            this.originalHeight = originalHeight;
            return this;
        }

        /**
         * Builder method for orientation parameter.
         *
         * @param orientation
         *            field to set
         * @return builder
         */
        public Builder withOrientation(int orientation) {
            this.orientation = orientation;
            return this;
        }

        /**
         * Builder method for jpegContent parameter.
         *
         * @param jpegContent
         *            field to set
         * @return builder
         */
        public Builder withJpegContent(byte[] jpegContent) {
            this.jpegContent = jpegContent;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImageVersionDto build() { return new ImageVersionDto(this); }
    }

}
