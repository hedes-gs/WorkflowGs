package com.workflow.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;

import org.apache.avro.reflect.Nullable;

public class HbaseImagesOfMetadata extends HbaseData implements Comparable<HbaseImagesOfMetadata> {
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "region_salt", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected Short           metaDataRegionSalt;
    @Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
    protected long            creationDate;
    @Column(hbaseName = "region_salt", isPartOfRowkey = true, rowKeyNumber = 3, toByte = ToByteShort.class, fixedWidth = ModelConstants.FIXED_WIDTH_SHORT)
    protected Short           regionSalt;
    @Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 4, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected String          imageId;
    // Data

    @Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
    protected String          imageName        = "";
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
    protected String          thumbName        = "";

    @Column(hbaseName = "path", rowKeyNumber = 103, toByte = ToByteString.class, columnFamily = "img")
    protected String          path             = "";
    @Column(hbaseName = "originalWidth", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalWidth;
    @Column(hbaseName = "originalHeight", rowKeyNumber = 105, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            originalHeight;
    @Column(hbaseName = "importDate", rowKeyNumber = 106, toByte = ToByteLong.class, columnFamily = "img")
    protected long            importDate;
    @Column(hbaseName = "orientation", toByte = ToByteLong.class, columnFamily = "img", rowKeyNumber = 107)
    protected long            orientation;

    @Nullable
    @Column(hbaseName = "lens", toByte = ToByteIdempotent.class, columnFamily = "tech", rowKeyNumber = 110)
    protected byte[]          lens;
    @Nullable
    @Column(hbaseName = "focalLens", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 111)
    protected int[]           focalLens;
    @Nullable
    @Column(hbaseName = "speed", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 112)
    protected int[]           speed;
    @Nullable
    @Column(hbaseName = "aperture", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 113)
    protected int[]           aperture;
    @Column(hbaseName = "isoSpeed", toByte = ToByteShort.class, columnFamily = "tech", rowKeyNumber = 114)
    protected short           isoSpeed;
    @Nullable
    @Column(hbaseName = "camera", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 115)
    protected String          camera;
    @Column(hbaseName = "shiftExpo", toByte = ToByteIntArray.class, columnFamily = "tech", rowKeyNumber = 116)
    @Nullable
    protected int[]           shiftExpo;
    @Column(hbaseName = "copyright", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 117)
    @Nullable
    protected String          copyright;
    @Column(hbaseName = "artist", toByte = ToByteString.class, columnFamily = "tech", rowKeyNumber = 118)
    @Nullable
    protected String          artist;
    @Column(hbaseName = HbaseImageThumbnail.TABLE_FAMILY_IMPORT_NAME, toByte = ToByteString.class, columnFamily = HbaseImageThumbnail.TABLE_FAMILY_IMPORT_NAME, rowKeyNumber = 119)
    @Nullable
    protected HashSet<String> importName;

    public HbaseImagesOfMetadata() { super(); }

    public HbaseImagesOfMetadata(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public long getCreationDate() { return this.creationDate; }

    public void setCreationDate(long creationDate) { this.creationDate = creationDate; }

    public String getImageId() { return this.imageId; }

    public void setImageId(String imageId) { this.imageId = imageId; }

    public String getImageName() { return this.imageName; }

    public void setImageName(String imageName) { this.imageName = imageName; }

    public String getThumbName() { return this.thumbName; }

    public void setThumbName(String thumbName) { this.thumbName = thumbName; }

    public String getPath() { return this.path; }

    public void setPath(String path) { this.path = path; }

    public long getOriginalWidth() { return this.originalWidth; }

    public void setOriginalWidth(long originalWidth) { this.originalWidth = originalWidth; }

    public long getOriginalHeight() { return this.originalHeight; }

    public void setOriginalHeight(long originalHeight) { this.originalHeight = originalHeight; }

    public long getImportDate() { return this.importDate; }

    public void setImportDate(long importDate) { this.importDate = importDate; }

    public long getOrientation() { return this.orientation; }

    public void setOrientation(long orientation) { this.orientation = orientation; }

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

    public HashSet<String> getImportName() { return this.importName; }

    public void setImportName(HashSet<String> importName) { this.importName = importName; }

    public static long getSerialversionuid() { return HbaseImagesOfMetadata.serialVersionUID; }

    public Short getRegionSalt() { return this.regionSalt; }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder = new StringBuilder();
        builder.append("HbaseImagesOfMetadata [creationDate=");
        builder.append(this.creationDate);
        builder.append(", imageId=");
        builder.append(this.imageId);
        builder.append(", imageName=");
        builder.append(this.imageName);
        builder.append(", thumbName=");
        builder.append(this.thumbName);
        builder.append(", path=");
        builder.append(this.path);
        builder.append(", originalWidth=");
        builder.append(this.originalWidth);
        builder.append(", originalHeight=");
        builder.append(this.originalHeight);
        builder.append(", importDate=");
        builder.append(this.importDate);
        builder.append(", orientation=");
        builder.append(this.orientation);
        builder.append(", lens=");
        builder.append(
            this.lens != null ? Arrays.toString(Arrays.copyOf(this.lens, Math.min(this.lens.length, maxLen))) : null);
        builder.append(", focalLens=");
        builder.append(
            this.focalLens != null
                ? Arrays.toString(Arrays.copyOf(this.focalLens, Math.min(this.focalLens.length, maxLen)))
                : null);
        builder.append(", speed=");
        builder.append(
            this.speed != null ? Arrays.toString(Arrays.copyOf(this.speed, Math.min(this.speed.length, maxLen)))
                : null);
        builder.append(", aperture=");
        builder.append(
            this.aperture != null
                ? Arrays.toString(Arrays.copyOf(this.aperture, Math.min(this.aperture.length, maxLen)))
                : null);
        builder.append(", isoSpeed=");
        builder.append(this.isoSpeed);
        builder.append(", camera=");
        builder.append(this.camera);
        builder.append(", shiftExpo=");
        builder.append(
            this.shiftExpo != null
                ? Arrays.toString(Arrays.copyOf(this.shiftExpo, Math.min(this.shiftExpo.length, maxLen)))
                : null);
        builder.append(", copyright=");
        builder.append(this.copyright);
        builder.append(", artist=");
        builder.append(this.artist);
        builder.append(", importName=");
        builder.append(this.importName);
        builder.append("]");
        return builder.toString();
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
            this.artist,
            this.camera,
            this.copyright,
            this.creationDate,
            this.imageId,
            this.imageName,
            this.importDate,
            this.importName,
            this.isoSpeed,
            this.orientation,
            this.originalHeight,
            this.originalWidth,
            this.path,
            this.thumbName);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImagesOfMetadata other = (HbaseImagesOfMetadata) obj;
        return Arrays.equals(this.aperture, other.aperture) && Objects.equals(this.artist, other.artist)
            && Objects.equals(this.camera, other.camera) && Objects.equals(this.copyright, other.copyright)
            && (this.creationDate == other.creationDate) && Arrays.equals(this.focalLens, other.focalLens)
            && Objects.equals(this.imageId, other.imageId) && Objects.equals(this.imageName, other.imageName)
            && (this.importDate == other.importDate) && Objects.equals(this.importName, other.importName)
            && (this.isoSpeed == other.isoSpeed) && Arrays.equals(this.lens, other.lens)
            && (this.orientation == other.orientation) && (this.originalHeight == other.originalHeight)
            && (this.originalWidth == other.originalWidth) && Objects.equals(this.path, other.path)
            && Arrays.equals(this.shiftExpo, other.shiftExpo) && Arrays.equals(this.speed, other.speed)
            && Objects.equals(this.thumbName, other.thumbName);
    }

    private String toString(Collection<?> collection, int maxLen) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int i = 0;
        for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && (i < maxLen); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(iterator.next());
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int compareTo(HbaseImagesOfMetadata o) {
        return Comparator.comparing(HbaseImagesOfMetadata::getCreationDate)
            .thenComparing(HbaseImagesOfMetadata::getImageName)
            .compare(this, o);
    }

}
