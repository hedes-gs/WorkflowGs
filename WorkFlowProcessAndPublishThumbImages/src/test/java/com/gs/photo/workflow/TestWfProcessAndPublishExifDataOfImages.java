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
import org.junit.After;
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
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.storm.FinalImage;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
public class TestWfProcessAndPublishExifDataOfImages {

    private static final short              EXIF_CREATION_DATE_ID = ApplicationConfig.EXIF_CREATION_DATE_ID;
    private static final String             DATE_1                = "Y:2019";
    private static final String             DATE_2                = "Y:2019/M:10";
    private static final String             DATE_3                = "Y:2019/M:10/D:16";
    private static final String             DATE_4                = "Y:2019/M:10/D:16/H:22";
    private static final String             DATE_5                = "Y:2019/M:10/D:16/H:22/Mn:12";
    private static final String             DATE_6                = "Y:2019/M:10/D:16/H:22/Mn:12/S:0";
    private static final Collection<String> DATES                 = ImmutableSet.of(
        TestWfProcessAndPublishExifDataOfImages.DATE_1,
        TestWfProcessAndPublishExifDataOfImages.DATE_2,
        TestWfProcessAndPublishExifDataOfImages.DATE_3,
        TestWfProcessAndPublishExifDataOfImages.DATE_4,
        TestWfProcessAndPublishExifDataOfImages.DATE_5,
        TestWfProcessAndPublishExifDataOfImages.DATE_6);

    private static final String             IMAGE_KEY             = "1234";                                 // KeysBuilder.topicThumbKeyBuilder()
    // .withOriginalImageKey("1234")
    // .withPathInExifTags(new short[] { 0, 1 })
    // .withThumbNb(1)
    // .build();
    private static final String             PATH                  = "/tmp/image/1234.ARW";
    private static final int                HEIGHT                = 768;
    private static final int                WIDTH                 = 1024;
    private static final byte[]             COMPRESSED_DATA       = new byte[] { 0, 1, 2, 3, 4 };
    private static final String             EXIF_DATE             = "2019:10:16 22:12:00";
    protected Properties                    props                 = new Properties();;

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

    @Value("${topic.topicCountOfImagesPerDate}")
    protected String                        topicImageDate;

    protected TopologyTestDriver            testDriver;

    public static void setUpBeforeClass() throws Exception {}

    @After
    public void close() { this.testDriver.close(); }

    @Before
    public void setUp() throws Exception {
        final File directoryToBeDeleted = new File("./tmp/test-kafkastreams");
        if (directoryToBeDeleted.exists()) {
            this.deleteDirectory(directoryToBeDeleted);
        }
        this.props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/test-kafkastreams");
        this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        this.testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

            @Override
            public void close() {
                try {
                    super.close();
                } catch (Exception e) {
                }
            }

        };
    }

    @Test
    public void test001_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234-IMG-0442a1ac97057fd5a29bce43d2f5d696", outputRecord.key());
        outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);
    }

    @Test
    public void test002_shouldRetrieveOneHbaseImageThumbnailWithRightValueWhenExifImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertEquals("1234-IMG-0442a1ac97057fd5a29bce43d2f5d696", outputRecord.key());
        Assert.assertEquals(
            DateTimeHelper.toEpochMillis(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE),
            outputRecord.value()
                .getCreationDate());
        Assert.assertArrayEquals(
            TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA,
            outputRecord.value()
                .getThumbnail());
        Assert.assertEquals(
            outputRecord.value()
                .getWidth(),
            TestWfProcessAndPublishExifDataOfImages.WIDTH);
        Assert.assertEquals(
            outputRecord.value()
                .getHeight(),
            TestWfProcessAndPublishExifDataOfImages.HEIGHT);
        Assert.assertEquals(
            outputRecord.value()
                .getPath(),
            TestWfProcessAndPublishExifDataOfImages.PATH);
    }

    @Test
    public void test003_shouldRetrieveNullFromOutputTopicWhenTopicExifIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);
    }

    @Test
    public void test004_shouldRetrieveNullFromOutputTopicWhenTopicDupFilteredFileIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);

    }

    @Test
    public void test005_shouldRetrieveNullFromOutputTopicWhenTopicFinalImageIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");

        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });

        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputDupFilteredFile);
        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);
    }

    @Test
    public void test006_shouldRetrieveOneHbaseImageThumbnailWhen2ExifOneImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputExchangedTiffDataWithOtherKey = this
            .createConsumerRecordForTopicExifData(factoryForExchangedTiffData, key, (short) 0, new short[] { 0, 1 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputExchangedTiffDataWithOtherKey);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234-IMG-0442a1ac97057fd5a29bce43d2f5d696", outputRecord.key());
        ProducerRecord<String, HbaseImageThumbnail> outputRecord2 = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord2);
    }

    @Test
    public void test007_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndTwoFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });

        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputFinalImageWithSalt = this.createConsumerRecordForTopicTransformedThumb(
            factoryForFinalImage,
            KeysBuilder.topicThumbKeyBuilder()
                .withOriginalImageKey("1235")
                .withThumbNb(1)
                .build());

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputFinalImageWithSalt);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234-IMG-0442a1ac97057fd5a29bce43d2f5d696", outputRecord.key());
        outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);
    }

    @Test
    public void test008_shouldRetrieveOneHbaseImageThumbnailWhenOneExifTwoImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile2 = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1235");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });

        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputDupFilteredFile2);
        this.testDriver.pipeInput(inputExchangedTiffData);

        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234-IMG-0442a1ac97057fd5a29bce43d2f5d696", outputRecord.key());
        outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNull(outputRecord);
    }

    @Test
    public void test009_shouldRetrieveDatesWhenExifImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            Serdes.String()
                .serializer());
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData = new ConsumerRecordFactory<>(
            this.topicExif,
            Serdes.String()
                .serializer(),
            new ExchangedDataSerializer());
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage = new ConsumerRecordFactory<>(
            this.topicTransformedThumb,
            Serdes.String()
                .serializer(),
            new FinalImageSerializer());

        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(factoryForTopicDupFilteredFile, "1234");
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            TestWfProcessAndPublishExifDataOfImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputExchangedTiffData);
        this.testDriver.pipeInput(inputFinalImage);

        ProducerRecord<String, Long> outputRecord = this.testDriver.readOutput(
            this.topicImageDate,
            Serdes.String()
                .deserializer(),
            Serdes.Long()
                .deserializer());
        boolean end = false;

        do {
            Assert.assertThat(outputRecord.key(), Matchers.isIn(TestWfProcessAndPublishExifDataOfImages.DATES));
            Assert.assertEquals(
                1,
                outputRecord.value()
                    .intValue());
            outputRecord = this.testDriver.readOutput(
                this.topicImageDate,
                Serdes.String()
                    .deserializer(),
                Serdes.Long()
                    .deserializer());
            end = outputRecord == null;

        } while (!end);
    }

    private ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicTransformedThumb(
        ConsumerRecordFactory<String, FinalImage> factoryForFinalImage,
        String key
    ) {
        FinalImage.Builder builder = FinalImage.builder();
        builder.withHeight(TestWfProcessAndPublishExifDataOfImages.HEIGHT)
            .withWidth(TestWfProcessAndPublishExifDataOfImages.WIDTH)
            .withCompressedData(TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA)
            .withVersion((short) 1)
            .withDataId("id");
        FinalImage finalImage = builder.build();
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForFinalImage.create(
            this.topicTransformedThumb,
            KeysBuilder.topicTransformedThumbKeyBuilder()
                .withOriginalImageKey(key)
                .withVersion(1)
                .build(),
            finalImage);
        return inputDupFilteredFile;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicDuFilteredFile(
        ConsumerRecordFactory<String, String> factoryForTopicDupFilteredFile,
        final String key
    ) {
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForTopicDupFilteredFile
            .create(this.topicDupFilteredFile, key, TestWfProcessAndPublishExifDataOfImages.PATH);
        return inputDupFilteredFile;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData,
        final String key,
        short tag,
        short[] path
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsByte(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE.getBytes(Charset.forName("UTF-8")))
            .withFieldType(FieldType.IFD)
            .withDataId(KeysBuilder.buildKeyForExifData(key, tag, path))
            .withImageId(key)
            .withIntId(1)
            .withKey(
                KeysBuilder.topicExifKeyBuilder()
                    .withOriginalImageKey(key)
                    .withTiffId(tag)
                    .withPath(path)
                    .build())
            .withLength(1234)
            .withTag(tag)
            .withPath(path)
            .withTotal(150);
        final ExchangedTiffData exchangedTiffData = builder.build();
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData
            .create(this.topicExif, exchangedTiffData.getKey(), exchangedTiffData);
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
