package com.gs.photo.workflow;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.collect.ImmutableSet;
import com.gs.photos.serializers.ExchangedDataSerializer;
import com.gs.photos.serializers.FinalImageSerializer;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.storm.FinalImage;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
public class TestWfProcessAndPublishExifDataOfImages {

	private static final String             DATE_1          = "Y:2019";
	private static final String             DATE_2          = "Y:2019/M:10";
	private static final String             DATE_3          = "Y:2019/M:10/D:16";
	private static final String             DATE_4          = "Y:2019/M:10/D:16/H:22";
	private static final String             DATE_5          = "Y:2019/M:10/D:16/H:22/Mn:12";
	private static final String             DATE_6          = "Y:2019/M:10/D:16/H:22/Mn:12/S:0";
	private static final Collection<String> DATES           = ImmutableSet.of(
			TestWfProcessAndPublishExifDataOfImages.DATE_1,
			TestWfProcessAndPublishExifDataOfImages.DATE_2,
			TestWfProcessAndPublishExifDataOfImages.DATE_3,
			TestWfProcessAndPublishExifDataOfImages.DATE_4,
			TestWfProcessAndPublishExifDataOfImages.DATE_5,
			TestWfProcessAndPublishExifDataOfImages.DATE_6);

	private static final String             IMAGE_KEY       = "1234-5678-ABCD";
	private static final String             PATH            = "/tmp/image/1234.ARW";
	private static final int                HEIGHT          = 768;
	private static final int                WIDTH           = 1024;
	private static final byte[]             COMPRESSED_DATA = new byte[] { 0, 1, 2, 3, 4 };
	private static final String             EXIF_DATE       = "2019-10-16 22:12:00.123";
	protected Properties                    props           = new Properties();;

	@Autowired
	protected Topology                      kafkaStreamsTopology;

	@Value("${topic.topicDupFilteredFile}")
	protected String                        topicDupFilteredFile;

	@Value("${topic.topicExif}")
	protected String                        topicExif;

	@Value("${topic.topicImageDataToPersist}")
	protected String                        topicImageDataToPersist;

	@Value("${topic.topicTransformedThumb}")
	protected String                        topicTransformedThumb;

	@Value("${topic.topicImageDate}")
	protected String                        topicImageDate;

	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		final File directoryToBeDeleted = new File("./tmp/test-kafkastreams");
		if (directoryToBeDeleted.exists()) {
			this.deleteDirectory(directoryToBeDeleted);
		}
		this.props.put(StreamsConfig.STATE_DIR_CONFIG,
				"./tmp/test-kafkastreams");
		this.props.put(StreamsConfig.APPLICATION_ID_CONFIG,
				"test");
		this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"dummy:1234");
	}

	@Test
	public void test001_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndOneFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
					outputRecord.key());
			outputRecord = testDriver.readOutput(this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);
		}
	}

	@Test
	public void test002_shouldRetrieveOneHbaseImageThumbnailWithRightValueWhenExifImagePathAndOneFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertEquals(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
					outputRecord.key());
			Assert.assertEquals(DateTimeHelper.toEpochMillis(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE),
					outputRecord.value().getCreationDate());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA,
					outputRecord.value().getThumbnail());
			Assert.assertEquals(outputRecord.value().getWidth(),
					TestWfProcessAndPublishExifDataOfImages.WIDTH);
			Assert.assertEquals(outputRecord.value().getHeight(),
					TestWfProcessAndPublishExifDataOfImages.HEIGHT);
			Assert.assertEquals(outputRecord.value().getPath(),
					TestWfProcessAndPublishExifDataOfImages.PATH);

		}
	}

	@Test
	public void test003_shouldRetrieveNullFromOutputTopicWhenTopicExifIsEmpty() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);
		}
	}

	@Test
	public void test004_shouldRetrieveNullFromOutputTopicWhenTopicDupFilteredFileIsEmpty() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);

		}
	}

	@Test
	public void test005_shouldRetrieveNullFromOutputTopicWhenTopicFinalImageIsEmpty() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);

			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);

			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputDupFilteredFile);
			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);

		}
	}

	@Test
	public void test006_shouldRetrieveOneHbaseImageThumbnailWhen2ExifOneImagePathAndOneFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffDataWithOtherKey = this
					.createConsumerRecordForTopicExifData(factoryForExchangedTiffData,
							key + "-slat");
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputExchangedTiffDataWithOtherKey);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
					outputRecord.key());
			outputRecord = testDriver.readOutput(this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);
		}
	}

	@Test
	public void test007_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndTwoFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);

			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImageWithSalt = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key + "salt");

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImageWithSalt);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
					outputRecord.key());
			outputRecord = testDriver.readOutput(this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);
		}
	}

	@Test
	public void test008_shouldRetrieveOneHbaseImageThumbnailWhenOneExifTwoImagePathAndOneFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputDupFilteredFile2 = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key + "salt");
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);

			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputDupFilteredFile2);
			testDriver.pipeInput(inputExchangedTiffData);

			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseImageThumbnail> outputRecord = testDriver.readOutput(
					this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
					outputRecord.key());
			outputRecord = testDriver.readOutput(this.topicImageDataToPersist,
					Serdes.String().deserializer(),
					new HbaseImageThumbnailSerDe().deserializer());
			Assert.assertNull(outputRecord);
		}
	}

	@Test
	public void test009_shouldRetrieveDatesWhenExifImagePathAndOneFinalImageArePublished() {
		try (
				TopologyTestDriver testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

					@Override
					public void close() {
						try {
							super.close();
						} catch (Exception e) {
						}
					}

				}) {
			final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
				this.topicDupFilteredFile,
				Serdes.String().serializer(),
				Serdes.String().serializer());
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new ExchangedDataSerializer());
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
				this.topicTransformedThumb,
				Serdes.String().serializer(),
				new FinalImageSerializer());

			ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile(
					factoryForTopicDupFilteredFile,
					key);
			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
					factoryForExchangedTiffData,
					key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(
					factoryForFinalImage,
					key);

			testDriver.pipeInput(inputDupFilteredFile);
			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, Long> outputRecord = testDriver.readOutput(this.topicImageDate,
					Serdes.String().deserializer(),
					Serdes.Long().deserializer());
			boolean end = false;

			do {
				Assert.assertThat(outputRecord.key(),
						Matchers.isIn(TestWfProcessAndPublishExifDataOfImages.DATES));
				Assert.assertEquals(1,
						outputRecord.value().intValue());
				outputRecord = testDriver.readOutput(this.topicImageDate,
						Serdes.String().deserializer(),
						Serdes.Long().deserializer());
				end = outputRecord == null;

			} while (!end);
		}
	}

	private ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicTransformedThumb(
			ConsumerRecordFactory<String, FinalImage> factoryForFinalImage, String key) {
		FinalImage.Builder builder = FinalImage.builder();
		builder.withHeight(TestWfProcessAndPublishExifDataOfImages.HEIGHT)
				.withWidth(TestWfProcessAndPublishExifDataOfImages.WIDTH)
				.withCompressedData(TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA).withOriginal(true);
		FinalImage finalImage = builder.build();
		ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForFinalImage.create(this.topicTransformedThumb,
				key,
				finalImage);
		return inputDupFilteredFile;
	}

	protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicDuFilteredFile(
			ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile, final String key) {
		ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForTopicDupFilteredFile.create(
				this.topicDupFilteredFile,
				key,
				TestWfProcessAndPublishExifDataOfImages.PATH);
		return inputDupFilteredFile;
	}

	protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
			ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData, final String key) {
		ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
		builder.withDataAsByte(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE.getBytes(Charset.forName("UTF-8")))
				.withFieldType(FieldType.IFD).withId("<id>").withImageId(key).withIntId(1).withKey("<key>")
				.withLength(1234).withTag(ApplicationConfig.EXIF_CREATION_DATE_ID).withTotal(150);
		final ExchangedTiffData exchangedTiffData = builder.build();
		ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData.create(this.topicExif,
				key,
				exchangedTiffData);
		return inputExchangedTiffData;
	}

	private void deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				this.deleteDirectory(file);
			}
		}
		directoryToBeDeleted.delete();
	}

}
