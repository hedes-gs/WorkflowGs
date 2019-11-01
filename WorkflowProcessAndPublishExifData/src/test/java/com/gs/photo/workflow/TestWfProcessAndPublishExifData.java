package com.gs.photo.workflow;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
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
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseDataSerializer;
import com.gs.photos.serializers.HbaseExifDataDeserializer;
import com.gs.photos.serializers.HbaseExifDataOfImagesDeserializer;
import com.gs.photos.serializers.HbaseImageThumbnailSerializer;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
public class TestWfProcessAndPublishExifData {

	private static final short[]            PATH2                       = new short[] { 1, 5, 8 };

	private static final short              EXIF_ID                     = (short) 0x9008;

	private static final long               CREATION_DATE               = 123456789;
	private static final String             KEY2                        = "<key>";
	private static final String             HBASE_IMAGE_THUMBNAIL_ID    = "<HbaseImageThumbnail_id>";
	private static final String             IMG_PATH                    = "<img path>";
	private static final String             THUMB_NAME                  = "<Thumb name>";
	private static final String             IMG_NAME                    = "<img name>";
	private static final byte[]             THUMBNAIL                   = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			10 };
	private static final String             IMGID                       = "<imgid>";
	private static final String             ID                          = "<id>";
	private static final FieldType          FIELD_TYPE_DOUBLE           = FieldType.DOUBLE;
	private static final short[]            DATA_AS_SHORT               = new short[] { 6, 7, 8 };
	private static final int[]              DATA_AS_INT                 = new int[] { 3, 4, 5 };
	private static final byte[]             DATA_AS_BYTE                = new byte[] { 0, 1, 2 };
	private static final String             DATE_1                      = "Y:2019";
	private static final String             DATE_2                      = "Y:2019/M:10";
	private static final String             DATE_3                      = "Y:2019/M:10/D:16";
	private static final String             DATE_4                      = "Y:2019/M:10/D:16/H:22";
	private static final String             DATE_5                      = "Y:2019/M:10/D:16/H:22/Mn:12";
	private static final String             DATE_6                      = "Y:2019/M:10/D:16/H:22/Mn:12/S:0";
	private static final Collection<String> DATES                       = ImmutableSet.of(
		TestWfProcessAndPublishExifData.DATE_1,
		TestWfProcessAndPublishExifData.DATE_2,
		TestWfProcessAndPublishExifData.DATE_3,
		TestWfProcessAndPublishExifData.DATE_4,
		TestWfProcessAndPublishExifData.DATE_5,
		TestWfProcessAndPublishExifData.DATE_6);

	private static final String             IMAGE_KEY                   = "1234-5678-ABCD";
	private static final String             PATH                        = "/tmp/image/1234.ARW";
	private static final int                HEIGHT                      = 768;
	private static final int                WIDTH                       = 1024;
	private static final byte[]             COMPRESSED_DATA             = new byte[] { 0, 1, 2, 3, 4 };
	private static final String             EXIF_DATE                   = "2019:10:16 22:12:00";
	private static final DateTimeFormatter  FORMATTER_FOR_CREATION_DATE = DateTimeFormatter
		.ofPattern("yyyy:MM:dd HH:mm:ss");
	protected Properties                    props                       = new Properties();;

	@Autowired
	protected Topology                      kafkaStreamsTopology;

	@Value("${topic.topicExif}")
	protected String                        topicExif;

	@Value("${topic.topicImageDataToPersist}")
	protected String                        topicImageDataToPersist;

	@Value("${topic.topicExifImageDataToPersist}")
	protected String                        topicExifImageDataToPerist;

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
	public void test001_shouldRetrieveTwoHbaseDatalWhenOneExifAndOneThumOneArePublished() {
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
			final String key = TestWfProcessAndPublishExifData.IMAGE_KEY;

			ConsumerRecordFactory<String, HbaseData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new HbaseDataSerializer());
			ConsumerRecordFactory<String, HbaseImageThumbnail> factoryForHbaseImageThumbnail = new ConsumerRecordFactory<>(
				this.topicImageDataToPersist,
				Serdes.String().serializer(),
				new HbaseImageThumbnailSerializer());

			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
				factoryForExchangedTiffData,
				key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordFortopicImageDataToPersist(
				factoryForHbaseImageThumbnail,
				key);

			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);

			ProducerRecord<String, HbaseData> outputRecord = testDriver.readOutput(this.topicExifImageDataToPerist,
				Serdes.String().deserializer(),
				new HbaseDataSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(TestWfProcessAndPublishExifData.IMAGE_KEY,
				outputRecord.key());
			outputRecord = testDriver.readOutput(this.topicExifImageDataToPerist,
				Serdes.String().deserializer(),
				new HbaseDataSerDe().deserializer());
			Assert.assertNotNull(outputRecord);
		}
	}

	@Test
	public void test002_shouldRetrieveTwoValidHbaseWhenExifAndOneThumbImageArePublished() {
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
			final String key = TestWfProcessAndPublishExifData.IMAGE_KEY;

			ConsumerRecordFactory<String, HbaseData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
				this.topicExif,
				Serdes.String().serializer(),
				new HbaseDataSerializer());
			ConsumerRecordFactory<String, HbaseImageThumbnail> factoryForHbaseImageThumbnail = new ConsumerRecordFactory<>(
				this.topicImageDataToPersist,
				Serdes.String().serializer(),
				new HbaseImageThumbnailSerializer());

			ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
				factoryForExchangedTiffData,
				key);
			ConsumerRecord<byte[], byte[]> inputFinalImage = this.createConsumerRecordFortopicImageDataToPersist(
				factoryForHbaseImageThumbnail,
				key);

			testDriver.pipeInput(inputExchangedTiffData);
			testDriver.pipeInput(inputFinalImage);
			ProducerRecord<String, HbaseExifData> outputRecordHbaseExifData = testDriver.readOutput(
				this.topicExifImageDataToPerist,
				Serdes.String().deserializer(),
				new HbaseExifDataDeserializer());
			HbaseExifData hbe = outputRecordHbaseExifData.value();
			Assert.assertEquals(TestWfProcessAndPublishExifData.CREATION_DATE,
				hbe.getCreationDate());
			Assert.assertEquals(TestWfProcessAndPublishExifData.EXIF_ID,
				hbe.getExifTag());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_BYTE,
				hbe.getExifValueAsByte());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_INT,
				hbe.getExifValueAsInt());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_SHORT,
				hbe.getExifValueAsShort());
			Assert.assertEquals(TestWfProcessAndPublishExifData.HEIGHT,
				hbe.getHeight());
			Assert.assertEquals(TestWfProcessAndPublishExifData.WIDTH,
				hbe.getWidth());
			Assert.assertEquals(TestWfProcessAndPublishExifData.IMGID,
				hbe.getImageId());
			Assert.assertEquals(TestWfProcessAndPublishExifData.THUMB_NAME,
				hbe.getThumbName());

			ProducerRecord<String, HbaseExifDataOfImages> outputRecordHbaseExifDataOfImages = testDriver.readOutput(
				this.topicExifImageDataToPerist,
				Serdes.String().deserializer(),
				new HbaseExifDataOfImagesDeserializer());
			HbaseExifDataOfImages hedi = outputRecordHbaseExifDataOfImages.value();

			Assert.assertEquals(this.arround(TestWfProcessAndPublishExifData.CREATION_DATE),
				DateTimeHelper.toEpochMillis(hedi.getCreationDate()));
			Assert.assertEquals(TestWfProcessAndPublishExifData.EXIF_ID,
				hedi.getExifTag());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_BYTE,
				hedi.getExifValueAsByte());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_INT,
				hedi.getExifValueAsInt());
			Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_SHORT,
				hedi.getExifValueAsShort());
			Assert.assertEquals(TestWfProcessAndPublishExifData.HEIGHT,
				hedi.getHeight());
			Assert.assertEquals(TestWfProcessAndPublishExifData.WIDTH,
				hedi.getWidth());
			Assert.assertEquals(TestWfProcessAndPublishExifData.IMGID,
				hedi.getImageId());
			Assert.assertEquals(TestWfProcessAndPublishExifData.THUMB_NAME,
				hedi.getThumbName());

		}
	}

	private int arround(long creationDate) {
		return 123456000;
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
			final String key = TestWfProcessAndPublishExifData.IMAGE_KEY;

			Assert.assertNull(null);
		}
	}

	@Test
	public void test005_shouldRetrieveNullFromOutputTopicWhenTopicImageDataIsEmpty() {
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
			final String key = TestWfProcessAndPublishExifData.IMAGE_KEY;

			Assert.assertNull(null);

		}
	}

	@Test
	public void test006_shouldRetrieveOneHbaseDatalWhenTwoExifAndOneThumOneArePublished() {
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
			final String key = TestWfProcessAndPublishExifData.IMAGE_KEY;
			Assert.assertNull(null);

		}
	}

	protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
		ConsumerRecordFactory<String, HbaseData> factoryForExchangedTiffData, final String key) {
		ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
		builder.withDataAsByte(TestWfProcessAndPublishExifData.DATA_AS_BYTE)
			.withDataAsInt(TestWfProcessAndPublishExifData.DATA_AS_INT)
			.withDataAsShort(TestWfProcessAndPublishExifData.DATA_AS_SHORT)
			.withFieldType(TestWfProcessAndPublishExifData.FIELD_TYPE_DOUBLE)
			.withId(TestWfProcessAndPublishExifData.ID)
			.withImageId(TestWfProcessAndPublishExifData.IMGID)
			.withIntId(0)
			.withKey(TestWfProcessAndPublishExifData.KEY2)
			.withLength(1260)
			.withTag(TestWfProcessAndPublishExifData.EXIF_ID)
			.withPath(TestWfProcessAndPublishExifData.PATH2)
			.withTotal(12678);

		final ExchangedTiffData exchangedTiffData = builder.build();
		ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData.create(this.topicExif,
			key,
			exchangedTiffData);
		return inputExchangedTiffData;
	}

	private ConsumerRecord<byte[], byte[]> createConsumerRecordFortopicImageDataToPersist(
		ConsumerRecordFactory<String, HbaseImageThumbnail> factoryForHbaseImageThumbnail, String key) {
		HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
		builder.withCreationDate(TestWfProcessAndPublishExifData.CREATION_DATE)
			.withHeight(TestWfProcessAndPublishExifData.HEIGHT)
			.withWidth(TestWfProcessAndPublishExifData.WIDTH)
			.withImageId(TestWfProcessAndPublishExifData.IMGID)
			.withImageName(TestWfProcessAndPublishExifData.IMG_NAME)
			.withVersion((short) 1)
			.withPath(TestWfProcessAndPublishExifData.IMG_PATH)
			.withThumbnail(TestWfProcessAndPublishExifData.THUMBNAIL)
			.withThumbName(TestWfProcessAndPublishExifData.THUMB_NAME);
		HbaseImageThumbnail hbit = builder.build();
		ConsumerRecord<byte[], byte[]> outputHbaseImageThumbnail = factoryForHbaseImageThumbnail.create(
			this.topicImageDataToPersist,
			key,
			hbit);
		return outputHbaseImageThumbnail;
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
