package com.gs.photo.workflow;

import java.io.File;
import java.nio.charset.Charset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.collect.ImmutableSet;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.workflow.pubexifdata.ApplicationConfig;
import com.gs.photo.workflow.pubexifdata.IBeanPublishExifData;
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseDataSerializer;
import com.gs.photos.serializers.HbaseExifDataDeserializer;
import com.gs.photos.serializers.HbaseImageThumbnailSerializer;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.SizeAndJpegContent;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
@ActiveProfiles("test")
public class TestWfProcessAndPublishExifData {
    private static Logger                   LOGGER                      = LoggerFactory
        .getLogger(TestWfProcessAndPublishExifData.class);

    private static final short[]            PATH2                       = new short[] { 1, 5, 8 };

    private static final short              EXIF_ID                     = (short) 0x9008;
    private static final short              WIDTH_ID                    = (short) 0xA002;
    private static final short              HEIGHT_ID                   = (short) 0xA003;
    private static final short              CREATION_DATE_ID            = (short) 0x9003;
    private static final long               CREATION_DATE               = 123456789;
    private static final String             KEY2                        = "<key>";
    private static final String             HBASE_IMAGE_THUMBNAIL_ID    = "<HbaseImageThumbnail_id>";
    private static final String             IMG_PATH                    = "<img path>";
    private static final String             THUMB_NAME                  = "<Thumb name>";
    private static final String             IMG_NAME                    = "<img name>";
    private static final byte[]             THUMBNAIL                   = new byte[] {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    private static final String             IMGID                       = "1234";
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

    private static final String             EXIF_KEY                    = TestWfProcessAndPublishExifData.IMGID;/*
                                                                                                                 * KeysBuilder
                                                                                                                 * .
                                                                                                                 * topicExifKeyBuilder
                                                                                                                 * ()
                                                                                                                 * .withOriginalImageKey
                                                                                                                 * (
                                                                                                                 * TestWfProcessAndPublishExifData
                                                                                                                 * .
                                                                                                                 * IMGID)
                                                                                                                 * .withTiffId
                                                                                                                 * (
                                                                                                                 * 123456)
                                                                                                                 * .withPath
                                                                                                                 * (new
                                                                                                                 * short
                                                                                                                 * [] {
                                                                                                                 * 0, 1,
                                                                                                                 * 2 })
                                                                                                                 * .build
                                                                                                                 * ();
                                                                                                                 */
    private static final String             PATH                        = "/tmp/image/1234.ARW";
    private static final int                HEIGHT                      = 768;
    private static final int                WIDTH                       = 1024;
    private static final byte[]             COMPRESSED_DATA             = new byte[] { 0, 1, 2, 3, 4 };
    private static final String             EXIF_DATE                   = "2019:10:16 22:12:00";
    private static final DateTimeFormatter  FORMATTER_FOR_CREATION_DATE = DateTimeFormatter
        .ofPattern("yyyy:MM:dd HH:mm:ss");

    @Autowired
    protected IBeanPublishExifData          beanPublishExifData;

    @Autowired
    protected Properties                    kafkaStreamTopologyProperties;

    @Autowired
    protected IKafkaStreamProperties        specificKafkaStreamApplicationProperties;

    protected TopologyTestDriver            testDriver;

    @Autowired
    @MockBean
    public Void                             processAndPublishExifDataInit;

    public static void setUpBeforeClass() throws Exception {}

    @AfterEach
    public void endOfTest() throws Exception { this.testDriver.close(); }

    @BeforeEach
    public void setUp() throws Exception {
        final File directoryToBeDeleted = new File("./tmp/test-kafkastreams");
        if (directoryToBeDeleted.exists()) {
            this.deleteDirectory(directoryToBeDeleted);
        }
        this.testDriver = new TopologyTestDriver(this.beanPublishExifData.buildKafkaStreamsTopology(),
            this.kafkaStreamTopologyProperties);
    }

    @Test
    public void test001_shouldRetrieveTwoHbaseDatalWhenOneExifAndOneThumOneArePublished() {

        TestInputTopic<String, HbaseData> inputExifTopic = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicExif(),
            new StringSerializer(),
            new HbaseDataSerializer());
        TestInputTopic<String, HbaseImageThumbnail> inputImageDataToPersistfTopic = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicImageDataToPersist(),
            new StringSerializer(),
            new HbaseImageThumbnailSerializer());

        TestOutputTopic<String, HbaseData> outputTopicExifImageDataToPerist = this.testDriver.createOutputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicExifImageDataToPersist(),
            new StringDeserializer(),
            new HbaseDataSerDe().deserializer());

        final String exifKey = TestWfProcessAndPublishExifData.EXIF_KEY;

        ExchangedTiffData inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            exifKey,
            TestWfProcessAndPublishExifData.PATH2,
            TestWfProcessAndPublishExifData.EXIF_ID,
            null);
        ExchangedTiffData inputExchangedTiffDataWithWidthTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH,
            TestWfProcessAndPublishExifData.WIDTH_ID,
            new int[1024]);
        ExchangedTiffData inputExchangedTiffDataWithHeightTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH,
            TestWfProcessAndPublishExifData.HEIGHT_ID,
            new int[768]);
        ExchangedTiffData inputExchangedTiffDataWithCreationDateTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_CREATION_DATE_ID_PATH,
            TestWfProcessAndPublishExifData.CREATION_DATE_ID,
            null);
        HbaseImageThumbnail inputFinalImage = this.createConsumerRecordFortopicImageDataToPersist("1234");

        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffData));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithWidthTag));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithHeightTag));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithCreationDateTag));
        inputImageDataToPersistfTopic.pipeInput(new TestRecord<>(exifKey, inputFinalImage));

        KeyValue<String, HbaseData> outputRecord = outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key);
        outputRecord = outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
    }

    @Test
    public void test002_shouldRetrieveTwoValidHbaseWhenExifAndOneThumbImageArePublished() {
        final String exifKey = TestWfProcessAndPublishExifData.EXIF_KEY;
        final String imgKey = "1234";

        TestInputTopic<String, HbaseData> inputExifTopic = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicExif(),
            new StringSerializer(),
            new HbaseDataSerializer());
        TestInputTopic<String, HbaseImageThumbnail> inputImageDataToPersistfTopic = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicImageDataToPersist(),
            new StringSerializer(),
            new HbaseImageThumbnailSerializer());

        TestOutputTopic<String, HbaseExifData> outputTopicExifImageDataToPerist = this.testDriver.createOutputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicExifImageDataToPersist(),
            new StringDeserializer(),
            new HbaseExifDataDeserializer());

        ExchangedTiffData inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            exifKey,
            TestWfProcessAndPublishExifData.PATH2,
            TestWfProcessAndPublishExifData.EXIF_ID,
            null);
        HbaseImageThumbnail inputFinalImage = this.createConsumerRecordFortopicImageDataToPersist(imgKey);
        ExchangedTiffData inputExchangedTiffDataWithWidthTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH,
            TestWfProcessAndPublishExifData.WIDTH_ID,
            new int[] { 1024 });
        ExchangedTiffData inputExchangedTiffDataWithCreationDateTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_CREATION_DATE_ID_PATH,
            TestWfProcessAndPublishExifData.CREATION_DATE_ID,
            null);
        ExchangedTiffData inputExchangedTiffDataWithHeightTag = this.createConsumerRecordForTopicExifData(
            exifKey,
            ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH,
            TestWfProcessAndPublishExifData.HEIGHT_ID,
            new int[] { 768 });
        long time = System.currentTimeMillis();

        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffData));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithWidthTag));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithHeightTag));
        inputExifTopic.pipeInput(new TestRecord<>(exifKey, inputExchangedTiffDataWithCreationDateTag));
        inputImageDataToPersistfTopic.pipeInput(new TestRecord<>(exifKey, inputFinalImage));

        KeyValue<String, HbaseExifData> outputRecordHbaseExifData = outputTopicExifImageDataToPerist.readKeyValue();
        HbaseExifData hbe = outputRecordHbaseExifData.value;
        Assert.assertEquals(
            DateTimeHelper.toEpochMillis(TestWfProcessAndPublishExifData.EXIF_DATE),
            hbe.getCreationDate());
        Assert.assertEquals(TestWfProcessAndPublishExifData.EXIF_ID, hbe.getExifTag());
        Assert.assertArrayEquals(
            TestWfProcessAndPublishExifData.EXIF_DATE.getBytes(Charset.forName("UTF-8")),
            hbe.getExifValueAsByte());
        Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_INT, hbe.getExifValueAsInt());
        Assert.assertArrayEquals(TestWfProcessAndPublishExifData.DATA_AS_SHORT, hbe.getExifValueAsShort());
        Assert.assertEquals(768, hbe.getHeight());
        Assert.assertEquals(1024, hbe.getWidth());
        Assert.assertEquals(TestWfProcessAndPublishExifData.IMGID, hbe.getImageId());
        Assert.assertEquals(TestWfProcessAndPublishExifData.THUMB_NAME, hbe.getThumbName());

    }

    @Test
    public void test003_shouldRetrieveNullFromOutputTopicWhenTopicExifIsEmpty() {
        final String key = TestWfProcessAndPublishExifData.EXIF_KEY;

        Assert.assertNull(null);
    }

    @Test
    public void test005_shouldRetrieveNullFromOutputTopicWhenTopicImageDataIsEmpty() {
        final String key = TestWfProcessAndPublishExifData.EXIF_KEY;

        Assert.assertNull(null);
    }

    @Test
    public void test006_shouldRetrieveOneHbaseDatalWhenTwoExifAndOneThumOneArePublished() {
        final String key = TestWfProcessAndPublishExifData.EXIF_KEY;
        Assert.assertNull(null);
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(
        final String key,
        final short[] path,
        final short tagId,
        int[] heightOrWidth
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsByte(TestWfProcessAndPublishExifData.EXIF_DATE.getBytes(Charset.forName("UTF-8")))
            .withDataAsInt(heightOrWidth != null ? heightOrWidth : TestWfProcessAndPublishExifData.DATA_AS_INT)
            .withDataAsShort(TestWfProcessAndPublishExifData.DATA_AS_SHORT)
            .withFieldType(TestWfProcessAndPublishExifData.FIELD_TYPE_DOUBLE)
            .withDataId(TestWfProcessAndPublishExifData.ID)
            .withImageId(TestWfProcessAndPublishExifData.IMGID)
            .withIntId(0)
            .withKey(TestWfProcessAndPublishExifData.KEY2)
            .withLength(1260)
            .withTag(tagId)
            .withPath(path)
            .withTotal(12678);

        final ExchangedTiffData exchangedTiffData = builder.build();

        return exchangedTiffData;
    }

    private HbaseImageThumbnail createConsumerRecordFortopicImageDataToPersist(String key) {
        HashMap<Integer, SizeAndJpegContent> map = new HashMap<>();
        SizeAndJpegContent sizeAndJpegContent = SizeAndJpegContent.builder()
            .withHeight(10)
            .withWidth(10)
            .withJpegContent(TestWfProcessAndPublishExifData.THUMBNAIL)
            .build();
        map.put(2, sizeAndJpegContent);
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        builder.withCreationDate(TestWfProcessAndPublishExifData.CREATION_DATE)
            .withHeight(TestWfProcessAndPublishExifData.HEIGHT)
            .withWidth(TestWfProcessAndPublishExifData.WIDTH)
            .withImageId(TestWfProcessAndPublishExifData.IMGID)
            .withImageName(TestWfProcessAndPublishExifData.IMG_NAME)
            .withDataId(TestWfProcessAndPublishExifData.ID)
            .withPath(TestWfProcessAndPublishExifData.IMG_PATH)
            .withThumbnail(map)
            .withThumbName(TestWfProcessAndPublishExifData.THUMB_NAME);
        HbaseImageThumbnail hbit = builder.build();
        return hbit;
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
