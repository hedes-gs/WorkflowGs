package com.gs.photos.workflow.metadata.tiff;

import java.io.Serializable;

import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.exif.ExifTag;
import com.gs.photos.workflow.metadata.exif.GPSTag;
import com.gs.photos.workflow.metadata.exif.InteropTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public abstract class TiffField<T> implements Comparable<TiffField<?>>, Serializable {

    private static final long        serialVersionUID = 1L;
    private final short              tagValue;
    private final Tag                tag;
    protected SimpleAbstractField<T> underLayingField;

    protected int                    dataOffset;

    public TiffField(
        Tag tag,
        SimpleAbstractField<T> underLayingField,
        short tagValue,
        int dataOffset
    ) {
        this.tag = tag;
        this.underLayingField = underLayingField;
        this.tagValue = tagValue;
        this.dataOffset = dataOffset;
    }

    @Override
    public int compareTo(TiffField<?> that) { return (this.tag.getValue() & 0xffff) - (this.tag.getValue() & 0xffff); }

    public int getLength() { return this.underLayingField.getFieldLength(); }

    public short getFieldType() { return this.underLayingField.getType(); }

    public T getData() { return this.underLayingField.getData(); }

    /** Return an integer array representing TIFF long field */
    public int[] getDataAsLong() {
        throw new UnsupportedOperationException(
            "getDataAsLong() method is only supported by" + " short, long, and rational data types");
    }

    /**
     * @return a String representation of the field data
     */
    public abstract String getDataAsString();

    /**
     * Used to update field data when necessary.
     * <p>
     * This method should be called only after the field has been written to the
     * underlying RandomOutputStream.
     *
     * @return the stream position where actual data starts to write
     */
    public int getDataOffset() { return this.dataOffset; }

    public Tag getTag() { return this.tag; }

    @Override
    public String toString() {
        Tag tag = this.getTag();
        if ((tag == TiffTag.UNKNOWN) || (tag == ExifTag.UNKNOWN) || (tag == GPSTag.UNKNOWN)
            || (tag == InteropTag.UNKNOWN)) {
            return " At Offset " + this.dataOffset + " / " + tag.toString() + " [TiffTag value: " + tag + ", tagValue: "
                + Integer.toHexString(this.tagValue) + "] : " + this.getDataAsString();

        } else {
            return " At Offset " + this.dataOffset + " / " + tag.toString() + " [TiffTag value: " + tag
                + "] : [field type " + this.underLayingField.getClass() + " ] : Data : " + this.getDataAsString();

        }
    }

    public short getTagValue() { return this.tagValue; }

}