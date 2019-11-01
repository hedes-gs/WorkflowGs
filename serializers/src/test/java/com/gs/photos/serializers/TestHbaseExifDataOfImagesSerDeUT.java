package com.gs.photos.serializers;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.HbaseExifDataOfImages;

public class TestHbaseExifDataOfImagesSerDeUT {

	@Test
	public void test001_shouldSerializeAndDeserializeSuccessWithDefaultPojo() {
		HbaseExifDataOfImagesSerializer ser = new HbaseExifDataOfImagesSerializer();
		final HbaseExifDataOfImages.Builder builder = HbaseExifDataOfImages.builder();
		HbaseExifDataOfImages hbaseExifData = builder.withCreationDate("2019-20-12")
			.withHeight(768)
			.withWidth(1024)
			.withImageId("<img>")
			.withExifTag(25)
			.withThumbName("img-1")
			.withExifValueAsByte(new byte[] { 1, 2, 3 })
			.withExifValueAsInt(new int[] { 1, 2, 3, 4, 5, 6 })
			.withExifValueAsShort(new short[] { 1, 2, 3, 4, 5, 6, 7, 8 })
			.withExifPath(new short[] { 1, 2, 3 })
			.build();
		final HbaseExifDataOfImages data = builder.build();
		byte[] results = ser.serialize(null,
			data);

		HbaseExifDataOfImagesDeserializer deser = new HbaseExifDataOfImagesDeserializer();

		HbaseExifDataOfImages hit = deser.deserialize(null,
			results);

		Assert.assertNotNull(hit);

	}

	@Test
	public void test002_shouldSerializeAndDeserializeSuccessWithValuedPojo() {
		HbaseExifDataOfImagesSerializer ser = new HbaseExifDataOfImagesSerializer();
		final HbaseExifDataOfImages.Builder builder = HbaseExifDataOfImages.builder();
		HbaseExifDataOfImages hbaseExifData = builder.withCreationDate("2019-20-12")
			.withHeight(768)
			.withWidth(1024)
			.withImageId("<img>")
			.withExifTag(25)
			.withThumbName("img-1")
			.withExifValueAsByte(new byte[] { 1, 2, 3 })
			.withExifValueAsInt(new int[] { 1, 2, 3, 4, 5, 6 })
			.withExifValueAsShort(new short[] { 1, 2, 3, 4, 5, 6, 7, 8 })
			.withExifPath(new short[] { 1, 2, 3 })
			.build();
		byte[] results = ser.serialize(null,
			hbaseExifData);

		HbaseExifDataOfImagesDeserializer deser = new HbaseExifDataOfImagesDeserializer();

		HbaseExifDataOfImages hit = deser.deserialize(null,
			results);

		Assert.assertEquals(hbaseExifData,
			hit);
	}

}
