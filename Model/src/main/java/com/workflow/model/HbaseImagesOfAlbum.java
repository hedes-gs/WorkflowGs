package com.workflow.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

@HbaseTableName(value = "images_album", page_table = true)
public class HbaseImagesOfAlbum extends HbaseImagesOfMetadata {

    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "album_name", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_ALBUM_NAME)
    protected String          albumName;

    private HbaseImagesOfAlbum(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.creationDate = builder.creationDate;
        this.imageId = builder.imageId;
        this.imageName = builder.imageName;
        this.thumbName = builder.thumbName;
        this.path = builder.path;
        this.width = builder.width;
        this.height = builder.height;
        this.originalWidth = builder.originalWidth;
        this.originalHeight = builder.originalHeight;
        this.importDate = builder.importDate;
        this.orientation = builder.orientation;
        this.lens = builder.lens;
        this.focalLens = builder.focalLens;
        this.speed = builder.speed;
        this.aperture = builder.aperture;
        this.isoSpeed = builder.isoSpeed;
        this.camera = builder.camera;
        this.shiftExpo = builder.shiftExpo;
        this.copyright = builder.copyright;
        this.artist = builder.artist;
        this.importName = builder.importName;
        this.albumName = builder.albumName;
        this.regionSalt = builder.regionSalt;
    }

    public String getAlbumName() { return this.albumName; }

    public void setAlbumName(String albumName) { this.albumName = albumName; }

    public HbaseImagesOfAlbum() { super(null,
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
        result = (prime * result) + Objects.hash(this.albumName);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImagesOfAlbum other = (HbaseImagesOfAlbum) obj;
        return Objects.equals(this.albumName, other.albumName);
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder2 = new StringBuilder();
        builder2.append("HbaseImagesOfAlbum [albumName=");
        builder2.append(this.albumName);
        builder2.append(", creationDate=");
        builder2.append(this.creationDate);
        builder2.append(", imageId=");
        builder2.append(this.imageId);
        builder2.append(", imageName=");
        builder2.append(this.imageName);
        builder2.append(", thumbName=");
        builder2.append(this.thumbName);
        builder2.append(", path=");
        builder2.append(this.path);
        builder2.append(", width=");
        builder2.append(this.width);
        builder2.append(", height=");
        builder2.append(this.height);
        builder2.append(", originalWidth=");
        builder2.append(this.originalWidth);
        builder2.append(", originalHeight=");
        builder2.append(this.originalHeight);
        builder2.append(", importDate=");
        builder2.append(this.importDate);
        builder2.append(", orientation=");
        builder2.append(this.orientation);
        builder2.append(", lens=");
        builder2.append(
            this.lens != null ? Arrays.toString(Arrays.copyOf(this.lens, Math.min(this.lens.length, maxLen))) : null);
        builder2.append(", focalLens=");
        builder2.append(
            this.focalLens != null
                ? Arrays.toString(Arrays.copyOf(this.focalLens, Math.min(this.focalLens.length, maxLen)))
                : null);
        builder2.append(", speed=");
        builder2.append(
            this.speed != null ? Arrays.toString(Arrays.copyOf(this.speed, Math.min(this.speed.length, maxLen)))
                : null);
        builder2.append(", aperture=");
        builder2.append(
            this.aperture != null
                ? Arrays.toString(Arrays.copyOf(this.aperture, Math.min(this.aperture.length, maxLen)))
                : null);
        builder2.append(", isoSpeed=");
        builder2.append(this.isoSpeed);
        builder2.append(", camera=");
        builder2.append(this.camera);
        builder2.append(", shiftExpo=");
        builder2.append(
            this.shiftExpo != null
                ? Arrays.toString(Arrays.copyOf(this.shiftExpo, Math.min(this.shiftExpo.length, maxLen)))
                : null);
        builder2.append(", copyright=");
        builder2.append(this.copyright);
        builder2.append(", artist=");
        builder2.append(this.artist);
        builder2.append(", importName=");
        builder2.append(this.importName);
        builder2.append(", dataCreationDate=");
        builder2.append(this.dataCreationDate);
        builder2.append(", dataId=");
        builder2.append(this.dataId);
        builder2.append("]");
        return builder2.toString();
    }

    /**
     * Creates builder to build {@link HbaseImagesOfAlbum}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImagesOfAlbum}.
     */
    public static final class Builder {
        private long            dataCreationDate;
        private String          dataId;
        private long            creationDate;
        private String          imageId;
        private String          imageName;
        private String          thumbName;
        private String          path;
        private long            width;
        private long            height;
        private long            originalWidth;
        private long            originalHeight;
        private long            importDate;
        private long            orientation;
        private byte[]          lens;
        private int[]           focalLens;
        private int[]           speed;
        private int[]           aperture;
        private short           isoSpeed;
        private String          camera;
        private int[]           shiftExpo;
        private String          copyright;
        private String          artist;
        private HashSet<String> importName;
        private String          albumName;
        private short           regionSalt;

        private Builder() {}

        public Builder withThumbNailImage(HbaseImageThumbnail hbi) {
            return this.withAperture(hbi.getAperture())
                .withArtist(hbi.getArtist())
                .withCamera(hbi.getCamera())
                .withCopyright(hbi.getCopyright())
                .withCreationDate(hbi.getCreationDate())
                // .withDataCreationDate(hbi.getDataCreationDate())
                .withDataId(hbi.getDataId())
                .withFocalLens(hbi.getFocalLens())
                .withHeight(hbi.getHeight())
                .withImageId(hbi.getImageId())
                .withImageName(hbi.getImageName())
                .withImportDate(hbi.getImportDate())
                .withImportName(hbi.getImportName())
                .withIsoSpeed(hbi.getIsoSpeed())
                .withLens(hbi.getLens())
                .withOrientation(hbi.getOrientation())
                .withOriginalHeight(hbi.getOriginalHeight())
                .withOriginalWidth(hbi.getOriginalWidth())
                .withPath(hbi.getPath())
                .withShiftExpo(hbi.getShiftExpo())
                .withSpeed(hbi.getSpeed())
                .withThumbName(hbi.getThumbName())
                .withRegionSalt(hbi.getRegionSalt())
                .withWidth(hbi.getWidth());
        }

        public Builder withRegionSalt(short regionSalt) {
            this.regionSalt = regionSalt;
            return this;
        }

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
         * Builder method for lens parameter.
         *
         * @param lens
         *            field to set
         * @return builder
         */
        public Builder withLens(byte[] lens) {
            this.lens = lens;
            return this;
        }

        /**
         * Builder method for focalLens parameter.
         *
         * @param focalLens
         *            field to set
         * @return builder
         */
        public Builder withFocalLens(int[] focalLens) {
            this.focalLens = focalLens;
            return this;
        }

        /**
         * Builder method for speed parameter.
         *
         * @param speed
         *            field to set
         * @return builder
         */
        public Builder withSpeed(int[] speed) {
            this.speed = speed;
            return this;
        }

        /**
         * Builder method for aperture parameter.
         *
         * @param aperture
         *            field to set
         * @return builder
         */
        public Builder withAperture(int[] aperture) {
            this.aperture = aperture;
            return this;
        }

        /**
         * Builder method for isoSpeed parameter.
         *
         * @param isoSpeed
         *            field to set
         * @return builder
         */
        public Builder withIsoSpeed(short isoSpeed) {
            this.isoSpeed = isoSpeed;
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
         * Builder method for shiftExpo parameter.
         *
         * @param shiftExpo
         *            field to set
         * @return builder
         */
        public Builder withShiftExpo(int[] shiftExpo) {
            this.shiftExpo = shiftExpo;
            return this;
        }

        /**
         * Builder method for copyright parameter.
         *
         * @param copyright
         *            field to set
         * @return builder
         */
        public Builder withCopyright(String copyright) {
            this.copyright = copyright;
            return this;
        }

        /**
         * Builder method for artist parameter.
         *
         * @param artist
         *            field to set
         * @return builder
         */
        public Builder withArtist(String artist) {
            this.artist = artist;
            return this;
        }

        /**
         * Builder method for importName parameter.
         *
         * @param importName
         *            field to set
         * @return builder
         */
        public Builder withImportName(HashSet<String> importName) {
            this.importName = importName;
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
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImagesOfAlbum build() { return new HbaseImagesOfAlbum(this); }
    }

}
