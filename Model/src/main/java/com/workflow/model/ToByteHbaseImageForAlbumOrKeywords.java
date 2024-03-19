package com.workflow.model;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteHbaseImageForAlbumOrKeywords extends ToByte<HbaseImageForAlbumOrKeywords> {

    @Override
    public default byte[] convert(HbaseImageForAlbumOrKeywords p) {
        long creationDate = p.getCreationDate();
        byte[] imageId = p.getImageId()
            .getBytes(Charset.forName("UTF-8"));
        short version = p.getVersion();
        byte[] tumbnail = p.getTumbnail();
        byte[] retValue = new byte[Bytes.SIZEOF_LONG + ModelConstants.FIXED_WIDTH_IMAGE_ID + Bytes.SIZEOF_SHORT
            + tumbnail.length];
        Arrays.fill(retValue, (byte) 0x20);
        int offset = 0;
        offset = Bytes.putLong(retValue, offset, creationDate);
        System.arraycopy(imageId, 0, retValue, offset, imageId.length);
        offset = offset + ModelConstants.FIXED_WIDTH_IMAGE_ID;
        offset = Bytes.putShort(retValue, offset, version);
        System.arraycopy(tumbnail, 0, retValue, offset, tumbnail.length);
        return retValue;
    }

    @Override
    public default HbaseImageForAlbumOrKeywords fromByte(byte[] parameter, int offset, int length) {
        long creationDate = Bytes.toLong(parameter, offset);
        offset = offset + Bytes.SIZEOF_LONG;
        String imageId = new String(parameter, offset, ModelConstants.FIXED_WIDTH_IMAGE_ID, Charset.forName("UTF-8"));
        offset = offset + ModelConstants.FIXED_WIDTH_IMAGE_ID;
        short version = Bytes.toShort(parameter, offset);
        offset = offset + Bytes.SIZEOF_SHORT;
        byte[] thumbNail = new byte[(parameter.length - offset) + 1];
        System.arraycopy(parameter, offset, thumbNail, 0, thumbNail.length);

        return HbaseImageForAlbumOrKeywords.builder()
            .withCreationDate(creationDate)
            .withImageId(imageId)
            .withTumbnail(thumbNail)
            .withVersion(version)
            .build();
    }

    @Override
    public default HbaseImageForAlbumOrKeywords fromByte(byte[] parameter) {
        int offset = 0;
        long creationDate = Bytes.toLong(parameter, offset);
        offset = offset + Bytes.SIZEOF_LONG;
        String imageId = new String(parameter, offset, ModelConstants.FIXED_WIDTH_IMAGE_ID, Charset.forName("UTF-8"));
        offset = offset + ModelConstants.FIXED_WIDTH_IMAGE_ID;
        short version = Bytes.toShort(parameter, offset);
        offset = offset + Bytes.SIZEOF_SHORT;
        byte[] thumbNail = new byte[(parameter.length - offset) + 1];
        System.arraycopy(parameter, offset, thumbNail, 0, thumbNail.length);
        return HbaseImageForAlbumOrKeywords.builder()
            .withCreationDate(creationDate)
            .withImageId(imageId)
            .withTumbnail(thumbNail)
            .withVersion(version)
            .build();
    }

    @Override
    public default ToByte<HbaseImageForAlbumOrKeywords> getInstance() { return new ToByteHbaseImageForAlbumOrKeywords() {}; }

}
