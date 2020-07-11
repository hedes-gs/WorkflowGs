package com.gs.photos.serializers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.HbaseImageThumbnail;

public class TestHbaseImageThumbnailSerDeUT {

    @Test
    public void test001_shouldSerializeAndDeserializeSuccessWithDefaultPojo() {
        HbaseImageThumbnailSerializer ser = new HbaseImageThumbnailSerializer();
        final HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        builder.withImageId("<img>")
            .withImageName("<img test>")
            .withPath("<path>")
            .withDataId("<dataId>")
            .withThumbnail(new byte[] {})
            .withAlbums(new HashSet<>(Collections.singleton("Mon album")))
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("A9")
            .withCopyright("Granda solutions")
            .withFocalLens(new int[] { 3, 4 })
            .withImportName("Mon import")
            .withKeyWords(new HashSet<>(Arrays.asList("key1", "key2")))
            .withLens("lens".getBytes())
            .withShiftExpo(new int[] { 5, 6 })
            .withSpeed(new int[] { 7, 8 })
            .withThumbName("<>");
        final HbaseImageThumbnail data = builder.build();
        byte[] results = ser.serialize(null, data);

        HbaseImageThumbnailDeserializer deser = new HbaseImageThumbnailDeserializer();

        HbaseImageThumbnail hit = deser.deserialize(null, results);

        Assert.assertNotNull(hit);

    }

    @Test
    public void test002_shouldSerializeAndDeserializeSuccessWithValuedPojo() {
        HbaseImageThumbnailSerializer ser = new HbaseImageThumbnailSerializer();
        final HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        HbaseImageThumbnail hbaseImageThumbnail = builder.withCreationDate(100)
            .withHeight(768)
            .withWidth(1024)
            .withImageId("<img>")
            .withPath("/test")
            .withDataId("<dataId>")
            .withThumbnail(new byte[] { 0, 1, 2 })
            .withThumbName("img-1")
            .withImageName("<img name>")
            .withAlbums(new HashSet<>(Collections.singleton("Mon album")))
            .withAperture(new int[] { 0, 1 })
            .withArtist("Mwa")
            .withCamera("A9")
            .withCopyright("Granda solutions")
            .withFocalLens(new int[] { 3, 4 })
            .withImportName("Mon import")
            .withKeyWords(new HashSet<>(Arrays.asList("key1", "key2")))
            .withLens("lens".getBytes())
            .withShiftExpo(new int[] { 5, 6 })
            .withSpeed(new int[] { 7, 8 })
            .build();
        byte[] results = ser.serialize(null, hbaseImageThumbnail);

        HbaseImageThumbnailDeserializer deser = new HbaseImageThumbnailDeserializer();

        HbaseImageThumbnail hit = deser.deserialize(null, results);

        Assert.assertEquals(hbaseImageThumbnail, hit);
    }

}
