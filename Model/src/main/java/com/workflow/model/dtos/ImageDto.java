package com.workflow.model.dtos;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import javax.annotation.Generated;
import javax.xml.bind.annotation.XmlType;

@XmlType
public class ImageDto implements Serializable {

    private static final long serialVersionUID = 1L;
    protected ImageKeyDto     data;
    protected String          imageName;
    protected String          creationDateAsString;
    protected LocalDateTime   importDate;
    protected int             thumbnailWidth;
    protected int             thumbnailHeight;
    protected int             originalWidth;
    protected int             originalHeight;
    protected int             orientation;
    protected String          caption;
    protected String          iso;
    protected String          aperture;
    protected String          speed;
    protected String[]        keywords;
    protected String[]        persons;
    protected String          album;
    protected String          camera;
    protected String          lens;
    protected long            ratings;

    @Generated("SparkTools")
    private ImageDto(Builder builder) {
        this.data = builder.data;
        this.imageName = builder.imageName;
        this.creationDateAsString = builder.creationDateAsString;
        this.importDate = builder.importDate;
        this.thumbnailWidth = builder.thumbnailWidth;
        this.thumbnailHeight = builder.thumbnailHeight;
        this.originalWidth = builder.originalWidth;
        this.originalHeight = builder.originalHeight;
        this.orientation = builder.orientation;
        this.caption = builder.caption;
        this.iso = builder.iso;
        this.aperture = builder.aperture;
        this.speed = builder.speed;
        this.keywords = builder.keywords;
        this.persons = builder.persons;
        this.album = builder.album;
        this.camera = builder.camera;
        this.lens = builder.lens;
        this.ratings = builder.ratings;
    }

    public String[] getPersons() { return this.persons; }

    public void setPersons(String[] persons) { this.persons = persons; }

    public long getRatings() { return this.ratings; }

    public void setRatings(long ratings) { this.ratings = ratings; }

    public String[] getKeywords() { return this.keywords; }

    public void setKeywords(String[] keywords) { this.keywords = keywords; }

    public String getAlbum() { return this.album; }

    public void setAlbum(String album) { this.album = album; }

    public String getCamera() { return this.camera; }

    public void setCamera(String camera) { this.camera = camera; }

    public String getLens() { return this.lens; }

    public void setLens(String lens) { this.lens = lens; }

    public String getIso() { return this.iso; }

    public void setIso(String iso) { this.iso = iso; }

    public String getAperture() { return this.aperture; }

    public void setAperture(String aperture) { this.aperture = aperture; }

    public String getSpeed() { return this.speed; }

    public void setSpeed(String speed) { this.speed = speed; }

    public void setImportDate(LocalDateTime importDate) { this.importDate = importDate; }

    public ImageDto() {

    }

    public static long getSerialversionuid() { return ImageDto.serialVersionUID; }

    public String getImageId() { return this.data.imageId; }

    public String getImageName() { return this.imageName; }

    public int getThumbnailWidth() { return this.thumbnailWidth; }

    public int getThumbnailHeight() { return this.thumbnailHeight; }

    public int getOriginalWidth() { return this.originalWidth; }

    public int getOriginalHeight() { return this.originalHeight; }

    public String getCaption() { return this.caption; }

    public OffsetDateTime getCreationDate() { return this.data.creationDate; }

    public ImageKeyDto getData() { return this.data; }

    public LocalDateTime getImportDate() { return this.importDate; }

    public String getCreationDateAsString() { return this.creationDateAsString; }

    public void setCreationDateAsString(String creationDateAsString) {
        this.creationDateAsString = creationDateAsString;
    }

    public void setData(ImageKeyDto data) { this.data = data; }

    public void setImageName(String imageName) { this.imageName = imageName; }

    public void setThumbnailWidth(int thumbnailWidth) { this.thumbnailWidth = thumbnailWidth; }

    public void setThumbnailHeight(int thumbnailHeight) { this.thumbnailHeight = thumbnailHeight; }

    public void setOriginalWidth(int originalWidth) { this.originalWidth = originalWidth; }

    public void setOriginalHeight(int originalHeight) { this.originalHeight = originalHeight; }

    public void setCaption(String caption) { this.caption = caption; }

    public int getOrientation() { return this.orientation; }

    public void setOrientation(int orientation) { this.orientation = orientation; }

    /**
     * Creates builder to build {@link ImageDto}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImageDto}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private ImageKeyDto   data;
        private String        imageName;
        private String        creationDateAsString;
        private LocalDateTime importDate;
        private int           thumbnailWidth;
        private int           thumbnailHeight;
        private int           originalWidth;
        private int           originalHeight;
        private int           orientation;
        private String        caption;
        private String        iso;
        private String        aperture;
        private String        speed;
        private String[]      keywords;
        private String[]      persons;
        private String        album;
        private String        camera;
        private String        lens;
        private long          ratings;

        private Builder() {}

        /**
         * Builder method for data parameter.
         *
         * @param data
         *            field to set
         * @return builder
         */
        public Builder withData(ImageKeyDto data) {
            this.data = data;
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
         * Builder method for creationDateAsString parameter.
         *
         * @param creationDateAsString
         *            field to set
         * @return builder
         */
        public Builder withCreationDateAsString(String creationDateAsString) {
            this.creationDateAsString = creationDateAsString;
            return this;
        }

        /**
         * Builder method for importDate parameter.
         *
         * @param importDate
         *            field to set
         * @return builder
         */
        public Builder withImportDate(LocalDateTime importDate) {
            this.importDate = importDate;
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
         * Builder method for caption parameter.
         *
         * @param caption
         *            field to set
         * @return builder
         */
        public Builder withCaption(String caption) {
            this.caption = caption;
            return this;
        }

        /**
         * Builder method for iso parameter.
         *
         * @param iso
         *            field to set
         * @return builder
         */
        public Builder withIso(String iso) {
            this.iso = iso;
            return this;
        }

        /**
         * Builder method for aperture parameter.
         *
         * @param aperture
         *            field to set
         * @return builder
         */
        public Builder withAperture(String aperture) {
            this.aperture = aperture;
            return this;
        }

        /**
         * Builder method for speed parameter.
         *
         * @param speed
         *            field to set
         * @return builder
         */
        public Builder withSpeed(String speed) {
            this.speed = speed;
            return this;
        }

        /**
         * Builder method for keywords parameter.
         *
         * @param keywords
         *            field to set
         * @return builder
         */
        public Builder withKeywords(String[] keywords) {
            this.keywords = keywords;
            return this;
        }

        /**
         * Builder method for persons parameter.
         *
         * @param persons
         *            field to set
         * @return builder
         */
        public Builder withPersons(String[] persons) {
            this.persons = persons;
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
         * Builder method for camera parameter.
         *
         * @param camera
         *            field to set
         * @return builder
         */
        public Builder withCamera(String camera) {
            this.camera = camera;
            return this;
        }

        /**
         * Builder method for lens parameter.
         *
         * @param lens
         *            field to set
         * @return builder
         */
        public Builder withLens(String lens) {
            this.lens = lens;
            return this;
        }

        /**
         * Builder method for ratings parameter.
         *
         * @param ratings
         *            field to set
         * @return builder
         */
        public Builder withRatings(long ratings) {
            this.ratings = ratings;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImageDto build() { return new ImageDto(this); }
    }

}
