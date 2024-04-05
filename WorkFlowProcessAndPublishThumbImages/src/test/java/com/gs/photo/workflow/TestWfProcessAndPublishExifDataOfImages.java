package com.gs.photo.workflow;

import java.io.File;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
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

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.workflow.pubthumbimages.ApplicationConfig;
import com.gs.photo.workflow.pubthumbimages.IBeanPublishThumbImages;
import com.gs.photos.serializers.ExchangedDataSerializer;
import com.gs.photos.serializers.FileToProcessSerializer;
import com.gs.photos.serializers.FinalImageSerializer;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;
import com.workflow.model.storm.FinalImage;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = ApplicationConfig.class)
@ActiveProfiles("test")
public class TestWfProcessAndPublishExifDataOfImages {

    protected static Logger                                LOGGER          = LoggerFactory
        .getLogger(TestWfProcessAndPublishExifDataOfImages.class);

    private static final String                            DATE_1          = "Y:2019";
    private static final String                            DATE_2          = "Y:2019/M:01";
    private static final String                            DATE_3          = "Y:2019/M:01/D:16";
    private static final String                            DATE_4          = "Y:2019/M:01/D:16/H:22";
    private static final String                            DATE_5          = "Y:2019/M:01/D:16/H:22/Mn:12";
    private static final String                            DATE_6          = "Y:2019/M:01/D:16/H:22/Mn:12/S:00";

    private static final String                            IMAGE_KEY       = "1234";                            // KeysBuilder.topicThumbKeyBuilder()
    // .withOriginalImageKey("1234")
    // .withPathInExifTags(new short[] { 0, 1 })
    // .withThumbNb(1)
    // .build();
    private static final FileToProcess                     FILE_TO_PROCESS = FileToProcess.builder()
        .withUrl("nfs://ipc3:/path")
        .withName("Mon file")
        .withIsLocal(false)
        .withImageId(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY)
        .withImportEvent(
            ImportEvent.builder()
                .withAlbum("album")
                .withImportName("Mon import")
                .withKeyWords(Arrays.asList("kw1", "kw2"))
                .withScanners(Arrays.asList("sc1", "sc2"))
                .withScanFolder("/scan")
                .build())
        .build();                                                                                               // =
    // "/tmp/image/1234.ARW";
    private static final int                               HEIGHT          = 768;
    private static final int                               WIDTH           = 1024;
    private static final byte[]                            COMPRESSED_DATA = new byte[] { 0, 1, 2, 3, 4 };
    private static final String                            EXIF_DATE       = "2019:01:16 22:12:00";

    @Autowired
    @MockBean
    Void                                                   processAndPublishThumbImages;

    @Autowired
    protected IBeanPublishThumbImages                      beanPublishThumbImages;

    @Autowired
    protected IKafkaStreamProperties                       specificKafkaStreamApplicationProperties;

    protected TopologyTestDriver                           testDriver;
    @Autowired
    protected Properties                                   kafkaStreamTopologyProperties;

    protected TestInputTopic<String, ExchangedTiffData>    inputExifForExchangedTiffDataTopic;
    protected TestInputTopic<String, FinalImage>           inputTopicTransformedThumb;
    protected TestInputTopic<String, FileToProcess>        inputTopicDupFilteredFile;
    protected TestOutputTopic<String, HbaseImageThumbnail> outputTopicExifImageDataToPerist;
    protected TestOutputTopic<String, WfEvents>            outputTopicEvent;

    public static void setUpBeforeClass() throws Exception {}

    @BeforeEach
    public void setUp() {
        final File directoryToBeDeleted = new File(this.specificKafkaStreamApplicationProperties.getKafkaStreamDir());
        if ((directoryToBeDeleted.exists() && directoryToBeDeleted.getName()
            .startsWith("./tmp")) || directoryToBeDeleted.getName()
                .startsWith(".\\tmp")) {
            this.deleteDirectory(directoryToBeDeleted);
        }
        final Topology kafkaStreamsTopology = this.beanPublishThumbImages.buildKafkaStreamsTopology();
        this.testDriver = new TopologyTestDriver(kafkaStreamsTopology, this.kafkaStreamTopologyProperties);
        TestWfProcessAndPublishExifDataOfImages.LOGGER.info(
            "... {} - {}",
            this.testDriver.producedTopicNames(),
            kafkaStreamsTopology.describe()
                .toString());

        this.inputExifForExchangedTiffDataTopic = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicExif(),
            new StringSerializer(),
            new ExchangedDataSerializer());
        this.inputTopicTransformedThumb = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicTransformedThumb(),
            new StringSerializer(),
            new FinalImageSerializer());
        this.inputTopicDupFilteredFile = this.testDriver.createInputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicDupFilteredFile(),
            new StringSerializer(),
            new FileToProcessSerializer());
        this.outputTopicExifImageDataToPerist = this.testDriver.createOutputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicImageDataToPersist(),
            new StringDeserializer(),
            new HbaseImageThumbnailSerDe().deserializer());

        this.outputTopicEvent = this.testDriver.createOutputTopic(
            this.specificKafkaStreamApplicationProperties.getTopics()
                .topicEvent(),
            new StringDeserializer(),
            new WfEventsSerDe().deserializer());

    }

    @AfterEach
    public void closeAll() {

        final File directoryToBeDeleted = new File(this.specificKafkaStreamApplicationProperties.getKafkaStreamDir());
        TestWfProcessAndPublishExifDataOfImages.LOGGER.info("deleting {} ", directoryToBeDeleted);
        if ((directoryToBeDeleted.exists() && directoryToBeDeleted.getName()
            .startsWith("./tmp")) || directoryToBeDeleted.getName()
                .startsWith(".\\tmp")) {
            this.deleteDirectory(directoryToBeDeleted);
        }
        this.testDriver.close();
    }

    @Test
    public void test001_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this
            .createConsumerRecordForTopicDuFilteredFile(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY);
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);

        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());

        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        Assert.assertTrue(!this.outputTopicExifImageDataToPerist.isEmpty());
        KeyValue<String, HbaseImageThumbnail> outputRecord = this.outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key);
        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());
    }

    @Test
    public void test002_shouldRetrieveOneHbaseImageThumbnailWithRightValueWhenExifImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        KeyValue<String, HbaseImageThumbnail> outputRecord = this.outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertEquals("1234", outputRecord.key);
        Assert.assertEquals(
            DateTimeHelper.toEpochMillis(TestWfProcessAndPublishExifDataOfImages.EXIF_DATE),
            outputRecord.value.getCreationDate());
        Assert.assertArrayEquals(
            TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA,
            outputRecord.value.getThumbnail()
                .get(1)
                .getJpegContent());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS.getUrl(),
            outputRecord.value.getPath());
        Assert.assertEquals(
            TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS.getName(),
            outputRecord.value.getImageName());
        Assert.assertEquals(1024, outputRecord.value.getOriginalWidth());
        Assert.assertEquals(768, outputRecord.value.getOriginalHeight());
        Assert.assertEquals(1, outputRecord.value.getOrientation());
    }

    @Test
    public void test003_shouldRetrieveNullFromOutputTopicWhenTopicExifIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));

        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());

    }

    @Test
    public void test004_shouldRetrieveNullFromOutputTopicWhenTopicDupFilteredFileIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;
        ExchangedTiffData inputExchangedTiffData = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);

        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputExchangedTiffData));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));

        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());

    }

    @Test
    public void test005_shouldRetrieveNullFromOutputTopicWhenTopicFinalImageIsEmpty() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");

        ExchangedTiffData inputExchangedTiffData = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });

        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputExchangedTiffData));
        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());

    }

    @Test
    public void test006_shouldRetrieveOneHbaseImageThumbnailWhen2ExifOneImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputExchangedTiffDataWithOtherKey = this
            .createConsumerRecordForTopicExifData(key, (short) 0, new short[] { 0, 1 });
        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputExifForExchangedTiffDataTopic.pipeInput(
            new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputExchangedTiffDataWithOtherKey));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        KeyValue<String, HbaseImageThumbnail> outputRecord = this.outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key);
        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());
    }

    @Test
    public void test007_shouldRetrieveOneHbaseImageThumbnailWhenOneExifOneImagePathAndTwoFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;

        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        FinalImage inputFinalImageWithSalt = this.createConsumerRecordForTopicTransformedThumb(
            KeysBuilder.topicThumbKeyBuilder()
                .withOriginalImageKey("1235")
                .withThumbNb(1)
                .build());

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputTopicTransformedThumb.pipeInput(new TestRecord<>("1235", inputFinalImageWithSalt));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        KeyValue<String, HbaseImageThumbnail> outputRecord = this.outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key);
        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());
    }

    @Test
    public void test008_shouldRetrieveOneHbaseImageThumbnailWhenOneExifTwoImagePathAndOneFinalImageArePublished() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;
        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        FileToProcess inputDupFilteredFile2 = this.createConsumerRecordForTopicDuFilteredFile("1235");
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);

        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputTopicDupFilteredFile.pipeInput(new TestRecord<>("1235", inputDupFilteredFile2));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        KeyValue<String, HbaseImageThumbnail> outputRecord = this.outputTopicExifImageDataToPerist.readKeyValue();
        Assert.assertNotNull(outputRecord);
        Assert.assertEquals("1234", outputRecord.key);
        Assert.assertTrue(this.outputTopicExifImageDataToPerist.isEmpty());

    }

    @Test
    public void test010_topicEvent() {
        final String key = TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY;
        FileToProcess inputDupFilteredFile = this.createConsumerRecordForTopicDuFilteredFile("1234");
        ExchangedTiffData inputEtdCreationDate = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CREATION_DATE_ID,
            new short[] { (short) 0, (short) 0x8769 });
        FinalImage inputFinalImage = this.createConsumerRecordForTopicTransformedThumb(key);
        ExchangedTiffData inputEtdOrientation = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ORIENTATION,
            new short[] { (short) 0 },
            (short) 1);
        ExchangedTiffData inputEtdWidth = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_WIDTH,
            new short[] { (short) 0, (short) 0x8769 },
            1024);
        ExchangedTiffData inputEtdHeight = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SIZE_HEIGHT,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            "90mm".getBytes());
        ExchangedTiffData inputEtdFocalLens = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_FOCAL_LENS,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 4, 5 });

        ExchangedTiffData inputEtdShiftExpo = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SHIFT_EXPO,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 6, 7 });
        ExchangedTiffData inputEtdSpeedIso = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED_ISO,
            new short[] { (short) 0, (short) 0x8769 },
            (short) 640);
        ExchangedTiffData inputEtdAperture = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_APERTURE,
            new short[] { (short) 0, (short) 0x8769 },
            768);
        ExchangedTiffData inputEtdSpeed = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_SPEED,
            new short[] { (short) 0, (short) 0x8769 },
            new int[] { 1, 120 });
        ExchangedTiffData inputEtdCopyright = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_COPYRIGHT,
            new short[] { (short) 0 },
            "Granada".getBytes());
        ExchangedTiffData inputEtdArtist = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_ARTIST,
            new short[] { (short) 0 },
            "mwa".getBytes());
        ExchangedTiffData inputEtdCameraModel = this.createConsumerRecordForTopicExifData(

            key,
            IBeanPublishThumbImages.EXIF_CAMERA_MODEL,
            new short[] { (short) 0 },
            "A9".getBytes());
        ExchangedTiffData inputSonyExifLens = this.createConsumerRecordForTopicExifData(
            key,
            IBeanPublishThumbImages.SONY_EXIF_LENS,
            IBeanPublishThumbImages.SONY_EXIF_LENS_PATH,
            "A9".getBytes());

        this.inputTopicDupFilteredFile
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputDupFilteredFile));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCreationDate));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdOrientation));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdWidth));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdHeight));
        this.inputTopicTransformedThumb
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputFinalImage));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCameraModel));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdArtist));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdCopyright));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdFocalLens));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdShiftExpo));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeedIso));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdAperture));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputEtdSpeed));
        this.inputExifForExchangedTiffDataTopic
            .pipeInput(new TestRecord<>(TestWfProcessAndPublishExifDataOfImages.IMAGE_KEY, inputSonyExifLens));

        this.testDriver.advanceWallClockTime(Duration.ofMillis(101));
        KeyValue<String, WfEvents> outputRecord = null;
        boolean end = false;

        do {
            outputRecord = this.outputTopicEvent.readKeyValue();
            Assert.assertEquals(outputRecord.key, "1234");
            Assert.assertEquals(
                outputRecord.value.getEvents()
                    .size(),
                1);
            end = this.outputTopicEvent.isEmpty();
        } while (!end);
    }

    private FinalImage createConsumerRecordForTopicTransformedThumb(String key) {
        FinalImage.Builder builder = FinalImage.builder();
        builder.withHeight(TestWfProcessAndPublishExifDataOfImages.HEIGHT)
            .withWidth(TestWfProcessAndPublishExifDataOfImages.WIDTH)
            .withCompressedData(TestWfProcessAndPublishExifDataOfImages.COMPRESSED_DATA)
            .withVersion((short) 1)
            .withDataId("id");
        FinalImage finalImage = builder.build();
        return finalImage;
    }

    protected FileToProcess createConsumerRecordForTopicDuFilteredFile(final String key) {
        return TestWfProcessAndPublishExifDataOfImages.FILE_TO_PROCESS;
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(final String key, short tag, short[] path) {
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
        return exchangedTiffData;
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(
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
        return exchangedTiffData;
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(
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
        return exchangedTiffData;
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(
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
        return exchangedTiffData;
    }

    protected ExchangedTiffData createConsumerRecordForTopicExifData(
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
        return exchangedTiffData;
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
