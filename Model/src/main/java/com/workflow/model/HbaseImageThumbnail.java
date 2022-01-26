package com.workflow.model;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.avro.reflect.Nullable;

@HbaseTableName(value = "image_thumbnail", page_table = true)
public class HbaseImageThumbnail extends HbaseData implements Comparable<HbaseImageThumbnail> {

    public static final String                     TABLE_FAMILY_IMPORT_NAME          = "import";
    public static final byte[]                     TABLE_FAMILY_IMPORT_NAME_AS_BYTES = HbaseImageThumbnail.TABLE_FAMILY_IMPORT_NAME
        .getBytes(Charset.forName("UTF-8"));
    public static final String                     TABLE_FAMILY_ALBUMS               = "albums";
    public static final byte[]                     TABLE_FAMILY_ALBUMS_AS_BYTES      = "albums"
        .getBytes(Charset.forName("UTF-8"));
    public static final String                     TABLE_FAMILY_KEYWORDS             = "keywords";
    public static final byte[]                     TABLE_FAMILY_KEYWORDS_AS_BYTES    = "keywords"
        .getBytes(Charset.forName("UTF-8"));
    public static final String                     TABLE_FAMILY_RATINGS              = "ratings";
    public static final byte[]                     TABLE_FAMILY_RATINGS_AS_BYTES     = "ratings"
        .getBytes(Charset.forName("UTF-8"));
    public static final String                     TABLE_FAMILY_PERSONS              = "persons";
    public static final byte[]                     TABLE_FAMILY_PERSONS_AS_BYTES     = "persons"
        .getBytes(Charset.forName("UTF-8"));
    public static final String                     TABLE_FAMILY_THB                  = "thb";
    public static final byte[]                     TABLE_FAMILY_THB_AS_BYTES         = HbaseImageThumbnail.TABLE_FAMILY_THB
        .getBytes(Charset.forName("UTF-8"));

    private static final long                      serialVersionUID                  = 1L;

    // Row key
    @Column(hbaseName = "region_salt", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected short                                regionSalt;
    @Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
    protected long                                 creationDate;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String                               imageId;

    // Data
    @Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
    protected String                               imageName                         = "";
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
    protected String                               thumbName                         = "";
    @Column(hbaseName = "path", rowKeyNumber = 103, toByte = ToByteString.class, columnFamily = "img")
    protected String                               path                              = "";
    @Column(hbaseName = "width", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
    protected long                                 width;
    @Column(hbaseName = "height", rowKeyNumber = 105, toByte = ToByteLong.class, columnFamily = "sz")
    protected long                                 height;
    @Column(hbaseName = "originalWidth", rowKeyNumber = 106, toByte = ToByteLong.class, columnFamily = "sz")
    protected long                                 originalWidth;
    @Column(hbaseName = "originalHeight", rowKeyNumber = 107, toByte = ToByteLong.class, columnFamily = "sz")
    protected long                                 originalHeight;
    @Column(hbaseName = "importDate", rowKeyNumber = 108, toByte = ToByteLong.class, columnFamily = "img")
    protected long                                 importDate;
    @Column(hbaseName = "orientation", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 109)
    protected long                                 orientation;

    @Column(hbaseName = "thumbnail", toByte = ToByteObject.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_THB, rowKeyNumber = 102, mapKeyClass = Integer.class)
    protected HashMap<Integer, SizeAndJpegContent> thumbnail                         = new HashMap<>();

    @Nullable
    @Column(hbaseName = "lens", toByte = ToByteIdempotent.class, columnFamily = "tech", rowKeyNumber = 110)
    protected byte[]                               lens;
    @Nullable
    @Column(hbaseName = "focalLens", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 111)
    protected int[]                                focalLens;
    @Nullable
    @Column(hbaseName = "speed", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 112)
    protected int[]                                speed;
    @Nullable
    @Column(hbaseName = "aperture", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 113)
    protected int[]                                aperture;
    @Column(hbaseName = "isoSpeed", toByte = ToByteShort.class, columnFamily = "tech", rowKeyNumber = 114)
    protected short                                isoSpeed;
    @Nullable
    @Column(hbaseName = "camera", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 115)
    protected String                               camera;
    @Column(hbaseName = "shiftExpo", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 116)
    @Nullable
    protected int[]                                shiftExpo;
    @Column(hbaseName = "copyright", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 117)
    @Nullable
    protected String                               copyright;
    @Column(hbaseName = "artist", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 118)
    @Nullable
    protected String                               artist;

    @Column(hbaseName = HbaseImageThumbnail.TABLE_FAMILY_IMPORT_NAME, toByte = ToByteString.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_IMPORT_NAME, rowKeyNumber = 119)
    @Nullable
    protected HashSet<String>                      importName;

    @Column(hbaseName = HbaseImageThumbnail.TABLE_FAMILY_ALBUMS, toByte = ToByteString.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_ALBUMS, rowKeyNumber = 120)
    @Nullable
    protected HashSet<String>                      albums;

    @Nullable
    @Column(hbaseName = "keyWords", toByte = ToByteString.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_KEYWORDS, rowKeyNumber = 121)
    protected HashSet<String>                      keyWords;

    @Nullable
    @Column(hbaseName = "persons", toByte = ToByteString.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_PERSONS, rowKeyNumber = 123)
    protected HashSet<String>                      persons;

    @Nullable
    @Column(hbaseName = "ratings", toByte = ToByteLong.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_RATINGS, rowKeyNumber = 122)
    protected HashSet<Long>                        ratings;

    private HbaseImageThumbnail(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.regionSalt = builder.regionSalt;
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
        this.thumbnail = builder.thumbnail;
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
        this.albums = builder.albums;
        this.keyWords = builder.keyWords;
        this.persons = builder.persons;
        this.ratings = builder.ratings;
    }

    public short getRegionSalt() { return this.regionSalt; }

    public void setRegionSalt(short regionSalt) { this.regionSalt = regionSalt; }

    public HashSet<String> getPersons() { return this.persons; }

    public void setPersons(HashSet<String> persons) { this.persons = persons; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public long getCreationDate() { return this.creationDate; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public HashMap<Integer, SizeAndJpegContent> getThumbnail() { return this.thumbnail; }

    public void setThumbnail(HashMap<Integer, SizeAndJpegContent> thumbnail) { this.thumbnail = thumbnail; }

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

    public long getOrientation() { return this.orientation; }

    public void setOrientation(long orientation) { this.orientation = orientation; }

    public void setOriginalWidth(long originalWidth) { this.originalWidth = originalWidth; }

    public void setOriginalHeight(long originalHeight) { this.originalHeight = originalHeight; }

    public void setImportDate(long importDate) { this.importDate = importDate; }

    public byte[] getLens() { return this.lens; }

    public void setLens(byte[] lens) { this.lens = lens; }

    public int[] getFocalLens() { return this.focalLens; }

    public void setFocalLens(int[] focalLens) { this.focalLens = focalLens; }

    public int[] getSpeed() { return this.speed; }

    public void setSpeed(int[] speed) { this.speed = speed; }

    public int[] getAperture() { return this.aperture; }

    public void setAperture(int[] aperture) { this.aperture = aperture; }

    public short getIsoSpeed() { return this.isoSpeed; }

    public void setIsoSpeed(short isoSpeed) { this.isoSpeed = isoSpeed; }

    public String getCamera() { return this.camera; }

    public void setCamera(String camera) { this.camera = camera; }

    public int[] getShiftExpo() { return this.shiftExpo; }

    public void setShiftExpo(int[] shiftExpo) { this.shiftExpo = shiftExpo; }

    public String getCopyright() { return this.copyright; }

    public void setCopyright(String copyright) { this.copyright = copyright; }

    public String getArtist() { return this.artist; }

    public void setArtist(String artist) { this.artist = artist; }

    public HbaseImageThumbnail() { super(null,
        0); }

    public HashSet<String> getImportName() { return this.importName; }

    public void setImportName(HashSet<String> importName) { this.importName = importName; }

    public HashSet<String> getAlbums() { return this.albums; }

    public void setAlbums(HashSet<String> albums) { this.albums = albums; }

    public HashSet<String> getKeyWords() { return this.keyWords; }

    public void setKeyWords(HashSet<String> keyWords) { this.keyWords = keyWords; }

    public HashSet<Long> getRatings() { return this.ratings; }

    public void setRatings(HashSet<Long> ratings) { this.ratings = ratings; }

    @Override
    public Object clone() throws CloneNotSupportedException {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw e;
        }
    }

    @Override
    public int compareTo(HbaseImageThumbnail o) {
        return Comparator.comparing(HbaseImageThumbnail::getCreationDate)
            .thenComparing(HbaseImageThumbnail::getImageName)
            .compare(this, o);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Arrays.hashCode(this.aperture);
        result = (prime * result) + Arrays.hashCode(this.focalLens);
        result = (prime * result) + Arrays.hashCode(this.lens);
        result = (prime * result) + Arrays.hashCode(this.shiftExpo);
        result = (prime * result) + Arrays.hashCode(this.speed);
        result = (prime * result) + Objects.hash(
            this.albums,
            this.artist,
            this.camera,
            this.copyright,
            this.creationDate,
            this.height,
            this.imageId,
            this.imageName,
            this.importDate,
            this.importName,
            this.isoSpeed,
            this.keyWords,
            this.orientation,
            this.originalHeight,
            this.originalWidth,
            this.path,
            this.persons,
            this.ratings,
            this.regionSalt,
            this.thumbName,
            this.thumbnail,
            this.width);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImageThumbnail other = (HbaseImageThumbnail) obj;
        return Objects.equals(this.albums, other.albums) && Arrays.equals(this.aperture, other.aperture)
            && Objects.equals(this.artist, other.artist) && Objects.equals(this.camera, other.camera)
            && Objects.equals(this.copyright, other.copyright) && (this.creationDate == other.creationDate)
            && Arrays.equals(this.focalLens, other.focalLens) && (this.height == other.height)
            && Objects.equals(this.imageId, other.imageId) && Objects.equals(this.imageName, other.imageName)
            && (this.importDate == other.importDate) && Objects.equals(this.importName, other.importName)
            && (this.isoSpeed == other.isoSpeed) && Objects.equals(this.keyWords, other.keyWords)
            && Arrays.equals(this.lens, other.lens) && (this.orientation == other.orientation)
            && (this.originalHeight == other.originalHeight) && (this.originalWidth == other.originalWidth)
            && Objects.equals(this.path, other.path) && Objects.equals(this.persons, other.persons)
            && Objects.equals(this.ratings, other.ratings) && (this.regionSalt == other.regionSalt)
            && Arrays.equals(this.shiftExpo, other.shiftExpo) && Arrays.equals(this.speed, other.speed)
            && Objects.equals(this.thumbName, other.thumbName) && Objects.equals(this.thumbnail, other.thumbnail)
            && (this.width == other.width);
    }

    protected boolean compareMap(Map<Integer, SizeAndJpegContent> m) {
        for (Entry<Integer, SizeAndJpegContent> e : this.thumbnail.entrySet()) {
            Integer key = e.getKey();
            SizeAndJpegContent value = e.getValue();
            if (value == null) {
                if (!((m.get(key) == null) && m.containsKey(key))) { return false; }
            } else {
                if (!Objects.deepEquals(value, m.get(key))) { return false; }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder2 = new StringBuilder();
        builder2.append("HbaseImageThumbnail [creationDate=");
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
        builder2.append(", thumbnail=");
        builder2.append(this.thumbnail != null ? this.toString(this.thumbnail.entrySet(), maxLen) : null);
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
        builder2.append(", albums=");
        builder2.append(this.albums != null ? this.toString(this.albums, maxLen) : null);
        builder2.append(", keyWords=");
        builder2.append(this.keyWords != null ? this.toString(this.keyWords, maxLen) : null);
        builder2.append(", persons=");
        builder2.append(this.persons != null ? this.toString(this.persons, maxLen) : null);
        builder2.append(", ratings=");
        builder2.append(this.ratings != null ? this.toString(this.ratings, maxLen) : null);
        builder2.append("]");
        return builder2.toString();
    }

    private String toString(Collection<?> collection, int maxLen) {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("[");
        int i = 0;
        for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && (i < maxLen); i++) {
            if (i > 0) {
                builder2.append(", ");
            }
            builder2.append(iterator.next());
        }
        builder2.append("]");
        return builder2.toString();
    }

    /**
     * Creates builder to build {@link HbaseImageThumbnail}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageThumbnail}.
     */
    public static final class Builder {
        private long                                 dataCreationDate;
        private String                               dataId;
        private short                                regionSalt;
        private long                                 creationDate;
        private String                               imageId;
        private String                               imageName;
        private String                               thumbName;
        private String                               path;
        private long                                 width;
        private long                                 height;
        private long                                 originalWidth;
        private long                                 originalHeight;
        private long                                 importDate;
        private long                                 orientation;
        private HashMap<Integer, SizeAndJpegContent> thumbnail;
        private byte[]                               lens;
        private int[]                                focalLens;
        private int[]                                speed;
        private int[]                                aperture;
        private short                                isoSpeed;
        private String                               camera;
        private int[]                                shiftExpo;
        private String                               copyright;
        private String                               artist;
        private HashSet<String>                      importName;
        private HashSet<String>                      albums;
        private HashSet<String>                      keyWords;
        private HashSet<String>                      persons;
        private HashSet<Long>                        ratings;

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
         * Builder method for regionSalt parameter.
         *
         * @param regionSalt
         *            field to set
         * @return builder
         */
        public Builder withRegionSalt(short regionSalt) {
            this.regionSalt = regionSalt;
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
         * Builder method for thumbnail parameter.
         *
         * @param thumbnail
         *            field to set
         * @return builder
         */
        public Builder withThumbnail(HashMap<Integer, SizeAndJpegContent> thumbnail) {
            this.thumbnail = thumbnail;
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
         * Builder method for albums parameter.
         *
         * @param albums
         *            field to set
         * @return builder
         */
        public Builder withAlbums(HashSet<String> albums) {
            this.albums = albums;
            return this;
        }

        /**
         * Builder method for keyWords parameter.
         *
         * @param keyWords
         *            field to set
         * @return builder
         */
        public Builder withKeyWords(HashSet<String> keyWords) {
            this.keyWords = keyWords;
            return this;
        }

        /**
         * Builder method for persons parameter.
         *
         * @param persons
         *            field to set
         * @return builder
         */
        public Builder withPersons(HashSet<String> persons) {
            this.persons = persons;
            return this;
        }

        /**
         * Builder method for ratings parameter.
         *
         * @param ratings
         *            field to set
         * @return builder
         */
        public Builder withRatings(HashSet<Long> ratings) {
            this.ratings = ratings;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImageThumbnail build() { return new HbaseImageThumbnail(this); }
    }

    public final static int compareForSorting(HbaseImageThumbnail o1, HbaseImageThumbnail o2) {
        return Comparator.comparing(HbaseImageThumbnail::getCreationDate)
            .thenComparing((a, b) -> {
                System.out.println("Comparing " + a.getImageName() + " / " + b.getImageName());
                return a.getImageName()
                    .compareToIgnoreCase(b.getImageName());
            })
            .compare(o1, o2);
    }

}
