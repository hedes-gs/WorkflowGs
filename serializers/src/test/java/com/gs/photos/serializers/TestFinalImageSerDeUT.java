package com.gs.photos.serializers;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.storm.FinalImage;

public class TestFinalImageSerDeUT {

    @Test
    public void test001_shouldSerializeAndDeserializeSuccessWithDefaultPojo() {
        FinalImageSerializer ser = new FinalImageSerializer();
        final FinalImage.Builder builder = FinalImage.builder();
        builder.withDataId("<id>")
            .withCompressedData(new byte[] {});
        final FinalImage data = builder.build();
        byte[] results = ser.serialize(null, data);

        FinalImageDeserializer deser = new FinalImageDeserializer();

        FinalImage hit = deser.deserialize(null, results);

        Assert.assertNotNull(hit);

    }

    @Test
    public void test002_shouldSerializeAndDeserializeSuccessWithValuedPojo() {
        FinalImageSerializer ser = new FinalImageSerializer();
        final FinalImage.Builder builder = FinalImage.builder();
        FinalImage hbaseExifData = builder.withHeight(768)
            .withDataId("<finalImage>")
            .withWidth(1024)
            .withDataId("<img>")
            .withCompressedData(new byte[] { 0, 1, 2 })
            .build();
        byte[] results = ser.serialize(null, hbaseExifData);

        FinalImageDeserializer deser = new FinalImageDeserializer();

        FinalImage hit = deser.deserialize(null, results);

        Assert.assertEquals(hbaseExifData, hit);
    }

}
