package com.gs.photos.serializers;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.HbaseExifData;

public class TestHbaseExifDataSerDeUT {

    @Test
    public void test001_shouldSerializeAndDeserializeSuccessWithDefaultPojo() {
        HbaseExifDataSerializer ser = new HbaseExifDataSerializer();
        final HbaseExifData.Builder builder = HbaseExifData.builder();
        builder.withImageId("<imgId>")
            .withDataId("hbe-id")
            .withExifPath(new short[] {});

        final HbaseExifData data = builder.build();
        byte[] results = ser.serialize(null, data);

        HbaseExifDataDeserializer deser = new HbaseExifDataDeserializer();

        HbaseExifData hit = deser.deserialize(null, results);

        Assert.assertNotNull(hit);

    }

    @Test
    public void test002_shouldSerializeAndDeserializeSuccessWithValuedPojo() {
        HbaseExifDataSerializer ser = new HbaseExifDataSerializer();
        final HbaseExifData.Builder builder = HbaseExifData.builder();
        HbaseExifData hbaseExifData = builder.withCreationDate(100)
            .withHeight(768)
            .withWidth(1024)
            .withDataId("hbe-id")
            .withImageId("<img>")
            .withExifTag((short) 25)
            .withExifValueAsByte(new byte[] { 0, 1, 2 })
            .withThumbName("img-1")
            .withExifPath(new short[] { 0, 1, 2, 3, 4 })
            .build();
        byte[] results = ser.serialize(null, hbaseExifData);

        HbaseExifDataDeserializer deser = new HbaseExifDataDeserializer();

        HbaseExifData hit = deser.deserialize(null, results);

        Assert.assertEquals(hbaseExifData, hit);
    }

}
