package com.gs.photos.workflow.metadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.gs.photos.workflow.metadata.tiff.TiffField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class IFD {

    public static final class IFDContext {
        MutableInt currentImageNb = new MutableInt(1);

        public int getImageAndIncrement() { return this.currentImageNb.getAndIncrement(); }

    }

    private Map<Tag, IFD>               children          = new HashMap<Tag, IFD>();
    private Multimap<Tag, TiffField<?>> tiffFields        = ArrayListMultimap.create();
    private short[]                     path;
    private int                         ifdNumber;
    private int                         jpegImageLength   = -1;
    private int                         jpegImagePosition = -1;
    private byte[]                      jpegImage;
    private Tag                         tag;
    private int                         endOffset;
    private int                         startOffset;
    private int                         currentImageNumber;

    public IFD() {

    }

    public IFD(Tag tag) { this.tag = tag; }

    public int getTotalNumberOfTiffFields() {
        int nbOfTiffFields = this.tiffFields.size();
        int nbOFChildren = 0;
        if (this.children.size() > 0) {
            nbOFChildren = this.children.values()
                .stream()
                .mapToInt((e) -> e.getTotalNumberOfTiffFields())
                .sum();
        }
        return nbOfTiffFields + nbOFChildren;
    }

    public int getCurrentImageNumber() { return this.currentImageNumber; }

    public short[] getPath() { return this.path; }

    public void addChild(Tag tag, IFD child) { this.children.put(tag, child); }

    public void addField(TiffField<?> tiffField, IFDContext ifdContext) {
        if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT) {
            this.jpegImagePosition = ((int[]) tiffField.getData())[0];
        }
        if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT_LENGTH) {
            this.jpegImageLength = ((int[]) tiffField.getData())[0];
        }
        if ((this.jpegImageLength != -1) && (this.jpegImagePosition != -1) && (this.jpegImage == null)) {
            this.jpegImage = new byte[this.jpegImageLength];
            this.currentImageNumber = ifdContext.getImageAndIncrement();
        }
        this.tiffFields.put(tiffField.getTag(), tiffField);
    }

    public void addFields(Collection<TiffField<?>> tiffFields, IFDContext ifdContext) {
        for (TiffField<?> field : tiffFields) {
            this.addField(field, ifdContext);
        }
    }

    public IFD getChild(Tag tag) { return this.children.get(tag); }

    public Map<Tag, IFD> getChildren() { return Collections.unmodifiableMap(this.children); }

    public int getEndOffset() { return this.endOffset; }

    public Collection<IFD> getAllChildren() { return Collections.unmodifiableCollection(this.children.values()); }

    public boolean imageIsPresent() { return this.jpegImage != null; }

    public int getJpegImageLength() { return this.jpegImageLength; }

    public int getJpegImagePosition() { return this.jpegImagePosition; }

    public byte[] getJpegImage() { return this.jpegImage; }

    /**
     * Return a String representation of the field
     *
     * @param tag
     *            Tag for the field
     * @return a String representation of the field
     */
    public String getFieldAsString(Tag tag) {
        Collection<TiffField<?>> field = this.tiffFields.get(tag);
        return Iterables.toString(field);
    }

    /** Get all the fields for this IFD from the internal map. */
    public Collection<TiffField<?>> getFields() {
        return Collections.unmodifiableCollection(this.tiffFields.values());
    }

    public int getSize() { return this.tiffFields.size(); }

    public int getStartOffset() { return this.startOffset; }

    /** Remove all the entries from the IDF fields map */
    public void removeAllFields() { this.tiffFields.clear(); }

    public IFD removeChild(Tag tag) { return this.children.remove(tag); }

    @Override
    public String toString() { return "IFD [tag=" + this.tag + "]"; }

    public Tag getTag() { return this.tag; }

    public static Stream<TiffFieldAndPath> tiffFieldsAsStream(Stream<IFD> idfs) {
        short[] path = new short[0];
        MutableInt mutableInt = new MutableInt(1);
        return idfs.flatMap((ifd) -> ifd.tiffFieldsAsStream(path, mutableInt));
    }

    public static Stream<IFD> ifdsAsStream(Collection<IFD> ifds) {
        short[] path = new short[] {};
        MutableInt currentTiffNumber = new MutableInt(1);
        Stream<IFD> retValue = ifds.stream()
            .flatMap((ifd) -> ifd.ifdsAsStream(path, currentTiffNumber));
        return retValue;
    }

    protected Stream<IFD> ifdsAsStream(short[] path, MutableInt currentTiffNumber) {
        final short[] pathAsShort = Arrays.copyOf(path, path.length + 1);
        pathAsShort[pathAsShort.length - 1] = this.getTag()
            .getValue();
        this.path = pathAsShort;
        this.ifdNumber = currentTiffNumber.getAndIncrement();
        return Stream.concat(
            Collections.singleton(this)
                .stream(),
            this.children.values()
                .stream()
                .flatMap((c) -> c.ifdsAsStream(pathAsShort, currentTiffNumber)));
    }

    protected Stream<TiffFieldAndPath> tiffFieldsAsStream(final short[] path, MutableInt currentTiffNumber) {
        final short[] pathAsShort = Arrays.copyOf(path, path.length + 1);
        pathAsShort[pathAsShort.length - 1] = this.getTag()
            .getValue();
        Stream<TiffFieldAndPath> retValue = Stream.concat(
            this.children.values()
                .stream()
                .flatMap((ifd) -> this.tiffFieldsAsStream(pathAsShort, ifd, currentTiffNumber)),
            this.tiffFields.values()
                .stream()
                .map(
                    (t) -> TiffFieldAndPath.builder()
                        .withTiffField(t)
                        .withTiffNumber(currentTiffNumber.getAndIncrement())
                        .withPath(pathAsShort)
                        .build()));
        return retValue;

    }

    protected Stream<TiffFieldAndPath> tiffFieldsAsStream(final short[] path, IFD ifd, MutableInt currentTiffNumber) {
        return ifd.tiffFieldsAsStream(path, currentTiffNumber);
    }

    public int getIfdNumber() { return this.ifdNumber; }

    public static int getNbOfTiffFields(Stream<IFD> metaData) {
        final int nbOfTiffFields = metaData.mapToInt((e) -> e.getTotalNumberOfTiffFields())
            .sum();
        return nbOfTiffFields;
    }

    public static int getNbOfTiffFields(Collection<IFD> metaData) {
        final int nbOfTiffFields = metaData.stream()
            .mapToInt((e) -> e.getTotalNumberOfTiffFields())
            .sum();
        return nbOfTiffFields;
    }

}