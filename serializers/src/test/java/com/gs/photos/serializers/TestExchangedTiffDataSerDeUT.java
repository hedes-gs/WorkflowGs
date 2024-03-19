package com.gs.photos.serializers;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;

public class TestExchangedTiffDataSerDeUT {

    private static final short[] PATH          = new short[] { 10, 11, 12 };
    private static final short[] DATA_AS_SHORT = new short[] { 7, 8, 9 };
    private static final int[]   DATA_AS_INT   = new int[] { 4, 5, 6 };
    private static final byte[]  DATA_AS_BYTE  = new byte[] { 1, 2, 3 };

    @Test
    public void test001_shouldSerializeAndDeserializeSuccessWithDefaultPojo() {
        ExchangedDataSerializer ser = new ExchangedDataSerializer();
        final ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataId("<id>")
            .withDataAsByte(new byte[] {})
            .withDataAsInt(new int[] {})
            .withDataAsShort(new short[] {})
            .withFieldType(FieldType.ASCII)
            .withKey("<key>")
            .withImageId("<img id>")
            .withPath(new short[] {});
        final ExchangedTiffData data = builder.build();
        byte[] results = ser.serialize(null, data);

        ExchangedDataDeserializer deser = new ExchangedDataDeserializer();

        ExchangedTiffData hit = deser.deserialize(null, results);

        Assert.assertNotNull(hit);

    }

    @Test
    public void test002_shouldSerializeAndDeserializeSuccessWithValuedPojo() {
        ExchangedDataSerializer ser = new ExchangedDataSerializer();
        final ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataId("<id>")
            .withDataAsByte(TestExchangedTiffDataSerDeUT.DATA_AS_BYTE)
            .withDataAsInt(TestExchangedTiffDataSerDeUT.DATA_AS_INT)
            .withDataAsShort(TestExchangedTiffDataSerDeUT.DATA_AS_SHORT)
            .withFieldType(FieldType.ASCII)
            .withKey("<key>")
            .withPath(TestExchangedTiffDataSerDeUT.PATH)
            .withImageId("<img id>");
        final ExchangedTiffData data = builder.build();
        byte[] results = ser.serialize(null, data);

        ExchangedDataDeserializer deser = new ExchangedDataDeserializer();

        ExchangedTiffData hit = deser.deserialize(null, results);
        Assert.assertEquals(data, hit);
    }

}
