package com.gs.photo.workflow;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
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
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.collect.ImmutableSet;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.serializers.ExchangedDataSerializer;
import com.gs.photos.serializers.FileToProcessSerializer;
import com.gs.photos.serializers.FinalImageSerializer;
import com.gs.photos.serializers.HbaseImageThumbnailKeySerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;
import com.workflow.model.storm.FinalImage;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
public class TestWfProcessAndPublishExifDataOfImages {

    protected static Logger                 LOGGER          = LoggerFactory
        .getLogger(TestWfProcessAndPublishExifDataOfImages.class);

    private static final String             DATE_1          = "Y:2019";
    private static final String             DATE_2          = "Y:2019/M:01";
    private static final String             DATE_3          = "Y:2019/M:01/D:16";
    private static final String             DATE_4          = "Y:2019/M:01/D:16/H:22";
    private static final String             DATE_5          = "Y:2019/M:01/D:16/H:22/Mn:12";
    private static final String             DATE_6          = "Y:2019/M:01/D:16/H:22/Mn:12/S:00";
    private static final Collection<String> DATES           = ImmutableSet.of(
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_1,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)),
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_2,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)),
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_3,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)),
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_4,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)),
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_5,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)),
        StringUtils.rightPad(
            TestWfProcessAndPublishExifDataOfImages.DATE_6,
            AbstractHbaseStatsDAO.getKeyLength(KeyEnumType.ALL)));

    private static final String             IMAGE_KEY       = "1234";                            // KeysBuilder.topicThumbKeyBuilder()
    // .withOriginalImageKey("1234")
    // .withPathInExifTags(new short[] { 0, 1 })
    // .withThumbNb(1)
    // .build();
    private static final FileToProcess      FILE_TO_PROCESS = FileToProcess.builder()
        .withPath("/path")
        .withHost("IPC3")
        .withName("1234")
        .withImportEvent(
            ImportEvent.builder()
                .withAlbum("album")
                .withImportName("Mon import")
                .withKeyWords(Arrays.asList("kw1", "kw2"))
                .withScanners(Arrays.asList("sc1", "sc2"))
                .build())
        .build();                                                                                // =
    // "/tmp/image/1234.ARW";
    private static final int                HEIGHT          = 768;
    private static final int                WIDTH           = 1024;
    private static final byte[]             COMPRESSED_DATA = new byte[] { 0, 1, 2, 3, 4 };
    private static final String             EXIF_DATE       = "2019:01:16 22:12:00";
    protected Properties                    props           = new Properties();;

    @MockBean
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents>    producerForPublishingWfEvents;

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

    @Value("${topic.topicEvent}")
    protected String                        topicEvent;

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
        if (this.testDriver == null) {
            TestWfProcessAndPublishExifDataOfImages.LOGGER.info("Creating testDriver");
            this.testDriver = new TopologyTestDriver(this.kafkaStreamsTopology, this.props) {

                @Override
                public void close() {
                    try {
                        super.close();
                    } catch (Exception e) {
                    }
                }

            };
            MockitoAnnotations.initMocks(this);
        }
    }

    @Test
    public void test001_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);

        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImage);

        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key());
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertEquals("1234", outputRecord.key());
        Assert.assertEquals(
            DateTimeHelper.toEpochMillis(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE),
            outputRecord.value()
                .getCreationDate());
        Assert.assertArrayEquals(
            TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA,
            outputRecord.value()
                .getThumbnail());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.WIDTH,
            outputRecord.value()
                .getWidth());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.HEIGHT,
            outputRecord.value()
                .getHeight());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS.getPath(),
            outputRecord.value()
                .getPath());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS.getName(),
            outputRecord.value()
                .getImageName());
        Assert.assertEquals(
            1024,
            outputRecord.value()
                .getOriginalWidth());
        Assert.assertEquals(
            768,
            outputRecord.value()
                .getOriginalHeight());
        Assert.assertEquals(
            1,
            outputRecord.value()
                .getOrientation());
    }

    @Test
    public void test003_shouldRetrieveNullFromOutputTopicWhenTopicExifIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
            ApplicationConfig.EXIF_CREATION_DATE_ID,
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
            ApplicationConfig.EXIF_CREATION_DATE_ID,
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputExchangedTiffDataWithOtherKey = this
            .createConsumerRecordForTopicExifData(factoryForExchangedTiffData, key, (short) 0, new short[] { 0, 1 });
        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputExchangedTiffDataWithOtherKey);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key());
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        ConsumerRecord<byte[], byte[]> inputFinalImageWithSalt = this.createConsumerRecordForTopicTransformedThumb(
            factoryForFinalImage,
            KeysBuilder.topicThumbKeyBuilder()
                .withOriginalImageKey("1235")
                .withThumbNb(1)
                .build());

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImageWithSalt);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key());
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);

        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputDupFilteredFile2);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnail> outputRecord = this.testDriver.readOutput(
            this.topicImageDataToPersist,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailSerDe().deserializer());
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key());
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

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        ProducerRecord<String, HbaseImageThumbnailKey> outputRecord = this.testDriver.readOutput(
            this.topicImageDate,
            Serdes.String()
                .deserializer(),
            new HbaseImageThumbnailKeySerDe().deserializer());
        boolean end = false;

        do {
            Assert.assertThat(outputRecord.key(), Matchers.isIn(TestWfProcessAndPublishExifDataOfImages.DATES));
            Assert.assertEquals(
                TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY,
                outputRecord.value()
                    .getImageId());
            outputRecord = this.testDriver.readOutput(
                this.topicImageDate,
                Serdes.String()
                    .deserializer(),
                new HbaseImageThumbnailKeySerDe().deserializer());
            end = outputRecord == null;

        } while (!end);
    }

    @Test
    public void test010_topicEvent() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile = new ConsumerRecordFactory<>(
            this.topicDupFilteredFile,
            Serdes.String()
                .serializer(),
            new FileToProcessSerializer());
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
        ConsumerRecord<byte[], byte[]> inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ConsumerRecord<byte[], byte[]> inputFinalImage = this
            .createConsumerRecordForTopicTransformedThumb(factoryForFinalImage, key);
        ConsumerRecord<byte[], byte[]> inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ConsumerRecord<byte[], byte[]> inputEtdWidth = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ConsumerRecord<byte[], byte[]> inputEtdHeight = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdFocalLens = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ConsumerRecord<byte[], byte[]> inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ConsumerRecord<byte[], byte[]> inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ConsumerRecord<byte[], byte[]> inputEtdAperture = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ConsumerRecord<byte[], byte[]> inputEtdSpeed = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ConsumerRecord<byte[], byte[]> inputEtdCopyright = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdArtist = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ConsumerRecord<byte[], byte[]> inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            factoryForExchangedTiffData,
            key,
            ApplicationConfig.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        this.testDriver.pipeInput(inputDupFilteredFile);
        this.testDriver.pipeInput(inputEtdCreationDate);
        this.testDriver.pipeInput(inputEtdOrientation);
        this.testDriver.pipeInput(inputEtdWidth);
        this.testDriver.pipeInput(inputEtdHeight);
        this.testDriver.pipeInput(inputFinalImage);
        this.testDriver.pipeInput(inputEtdCameraModel);
        this.testDriver.pipeInput(inputEtdArtist);
        this.testDriver.pipeInput(inputEtdCopyright);
        this.testDriver.pipeInput(inputEtdLens);
        this.testDriver.pipeInput(inputEtdFocalLens);
        this.testDriver.pipeInput(inputEtdShiftExpo);
        this.testDriver.pipeInput(inputEtdSpeedIso);
        this.testDriver.pipeInput(inputEtdAperture);
        this.testDriver.pipeInput(inputEtdSpeed);

        this.testDriver.advanceWallClockTime(101);
        ProducerRecord<String, WfEvents> outputRecord = this.testDriver.readOutput(
            this.topicEvent,
            Serdes.String()
                .deserializer(),
            new WfEventsSerDe().deserializer());
        boolean end = false;

        do {
            Assert.assertEquals(outputRecord.key(), "1234");
            Assert.assertEquals(
                outputRecord.value()
                    .getEvents()
                    .size(),
                1);
            outputRecord = this.testDriver.readOutput(
                this.topicEvent,
                Serdes.String()
                    .deserializer(),
                new WfEventsSerDe().deserializer());
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
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForFinalImage
            .create(this.topicTransformedThumb, key, finalImage);
        return inputDupFilteredFile;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicDuFilteredFile(
        ConsumerRecordFactory<String, FileToProcess> factoryForTopicDupFilteredFile,
        final String key
    ) {
        ConsumerRecord<byte[], byte[]> inputDupFilteredFile = factoryForTopicDupFilteredFile
            .create(this.topicDupFilteredFile, key, TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS);
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
            .withKey(key)
            .withLength(1234)
            .withTag(tag)
            .withPath(path)
            .withTotal(150);
        final ExchangedTiffData exchangedTiffData = builder.build();
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData
            .create(this.topicExif, exchangedTiffData.getKey(), exchangedTiffData);
        return inputExchangedTiffData;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData,
        final String key,
        short tag,
        short[] path,
        int value
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsInt(new int[] { value })
            .withFieldType(FieldType.IFD)
            .withDataId(KeysBuilder.buildKeyForExifData(key, tag, path))
            .withImageId(key)
            .withIntId(1)
            .withKey(key)
            .withLength(1234)
            .withTag(tag)
            .withPath(path)
            .withTotal(150);
        final ExchangedTiffData exchangedTiffData = builder.build();
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData
            .create(this.topicExif, exchangedTiffData.getKey(), exchangedTiffData);
        return inputExchangedTiffData;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData,
        final String key,
        short tag,
        short[] path,
        int[] value
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsInt(value)
            .withFieldType(FieldType.IFD)
            .withDataId(KeysBuilder.buildKeyForExifData(key, tag, path))
            .withImageId(key)
            .withIntId(1)
            .withKey(key)
            .withLength(1234)
            .withTag(tag)
            .withPath(path)
            .withTotal(150);
        final ExchangedTiffData exchangedTiffData = builder.build();
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData
            .create(this.topicExif, exchangedTiffData.getKey(), exchangedTiffData);
        return inputExchangedTiffData;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData,
        final String key,
        short tag,
        short[] path,
        byte[] value
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsByte(value)
            .withFieldType(FieldType.IFD)
            .withDataId(KeysBuilder.buildKeyForExifData(key, tag, path))
            .withImageId(key)
            .withIntId(1)
            .withKey(key)
            .withLength(1234)
            .withTag(tag)
            .withPath(path)
            .withTotal(150);
        final ExchangedTiffData exchangedTiffData = builder.build();
        ConsumerRecord<byte[], byte[]> inputExchangedTiffData = factoryForExchangedTiffData
            .create(this.topicExif, exchangedTiffData.getKey(), exchangedTiffData);
        return inputExchangedTiffData;
    }

    protected ConsumerRecord<byte[], byte[]> createConsumerRecordForTopicExifData(
        ConsumerRecordFactory<String, ExchangedTiffData> factoryForExchangedTiffData,
        final String key,
        short tag,
        short[] path,
        short value
    ) {
        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withDataAsShort(new short[] { value })
            .withFieldType(FieldType.IFD)
            .withDataId(KeysBuilder.buildKeyForExifData(key, tag, path))
            .withImageId(key)
            .withIntId(1)
            .withKey(key)
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
