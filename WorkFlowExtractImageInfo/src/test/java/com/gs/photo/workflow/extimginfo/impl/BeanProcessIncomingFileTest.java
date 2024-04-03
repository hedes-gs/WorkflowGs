package com.gs.photo.workflow.extimginfo.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.WorkflowExtractImageInfo;
import com.gs.photo.workflow.extimginfo.ports.IAccessDirectlyFile;
import com.gs.photos.serializers.MultipleSerializers;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.files.FileToProcess;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@SpringBootTest(classes = { WorkflowExtractImageInfo.class })
class BeanProcessIncomingFileTest {
    protected static Logger LOGGER = LoggerFactory.getLogger(BeanProcessIncomingFileTest.class);
    static {

    }

    @MockBean
    private Ignite                                      ignite;

    @MockBean
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue;

    @MockBean
    protected Supplier<Producer<String, HbaseData>>     producerSupplierForTransactionPublishingOnExifTopic;

    protected MockConsumer<String, FileToProcess>       consumerForTopicWithFileToProcessValue;

    protected MockProducer<String, HbaseData>           producerForTransactionPublishingOnExifOrImageTopic;

    @Autowired
    @MockBean
    protected IIgniteDAO                                iIgniteDAO;

    @Autowired
    @MockBean
    protected IAccessDirectlyFile                       accessDirectlyFile;

    @Autowired
    protected BeanProcessIncomingFile                   beanProcessIncomingFile;

    @Autowired
    protected IFileMetadataExtractor                    beanFileMetadataExtractor;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    private MockProducer                                producerForTransactionPublishingOnExifOrImageTopic2;

    @Autowired
    @MockBean
    protected Void                                      startConsumers;

    @BeforeEach
    public void setUp() throws Exception {
        this.consumerForTopicWithFileToProcessValue = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        this.producerForTransactionPublishingOnExifOrImageTopic = new MockProducer<>(false,
            new StringSerializer(),
            new MultipleSerializers());
        this.producerForTransactionPublishingOnExifOrImageTopic2 = new MockProducer<>(false,
            new StringSerializer(),
            new MultipleSerializers());
        ByteBuffer bb = this.readBufferOfImage();
        Mockito.when(this.iIgniteDAO.isReady())
            .thenReturn(true);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
        Mockito.when(this.iIgniteDAO.get("2"))
            .thenReturn(Optional.ofNullable(null));
        Mockito.when(this.iIgniteDAO.get("3"))
            .thenReturn(Optional.ofNullable(null));

        Mockito.when(this.kafkaConsumerFactoryForFileToProcessValue.get())
            .thenReturn(this.consumerForTopicWithFileToProcessValue);
        Mockito.when(this.producerSupplierForTransactionPublishingOnExifTopic.get())
            .thenReturn(this.producerForTransactionPublishingOnExifOrImageTopic);
        this.beanProcessIncomingFile.start();
    }

    @AfterEach
    public void stop() throws Exception {
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(() -> {
            BeanProcessIncomingFileTest.LOGGER.info("Stoping test");
            throw new RuntimeException(new InterruptedException());

        });
    }

    @Test
    public void test001_shouldRetrieveNoCopyrightAndArtist() throws IOException {
        ByteBuffer bb = this.readBufferOfImage();
        final Collection<IFD> IFDs = this.beanFileMetadataExtractor
            .readIFDs(
                Optional.of(bb.array()),
                FileToProcess.builder()
                    .build())
            .get();

        List<TiffFieldAndPath> optionalParameters = IFD.tiffFieldsAsStream(IFDs.stream())
            .filter((ifd) -> this.checkIfOptionalParametersArePresent(ifd))
            .collect(Collectors.toList());
        Assert.assertEquals(0, optionalParameters.size());
    }

    @Test
    public void test001_1_shouldRetrieveDirectlyImageByte() throws IOException {
        ByteBuffer bb = this.readBufferOfImage();
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
                0,
                0,
                "1",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()),
            new ConsumerRecord<>(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
                0,
                1,
                "2",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()));
        Mockito.when(this.accessDirectlyFile.readFirstBytesOfFileRetry(ArgumentMatchers.any()))
            .thenReturn(Optional.of(bb.array()))
            .thenReturn(Optional.of(bb.array()));

        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.updateBeginningOffsets(
                Collections.singletonMap(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0),
                    0L)));

        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.rebalance(
                Collections.singleton(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0))));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(() -> {
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(0));
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(1));
        });
        this.waitForEndOfTransaction();

        Assert.assertEquals(
            this.producerForTransactionPublishingOnExifOrImageTopic.history()
                .size(),
            2 + (221 * 2));
    }

    private ByteBuffer readBufferOfImage() throws IOException {
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        return bb;
    }

    @Test
    public void test001_shouldRetrieveDefaultCopyrightAndArtist() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        ByteBuffer bb = this.readBufferOfImage();

        Mockito.when(this.accessDirectlyFile.readFirstBytesOfFileRetry(ArgumentMatchers.any()))
            .thenReturn(Optional.of(bb.array()))
            .thenReturn(Optional.of(bb.array()));
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
                0,
                0,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
                    .build()),
            new ConsumerRecord<>(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
                0,
                1,
                "2",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.updateBeginningOffsets(
                Collections.singletonMap(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0),
                    0L)));

        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.rebalance(
                Collections.singleton(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0))));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(() -> {
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(0));
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(1));
        });
        this.waitForEndOfTransaction();

        Assert.assertEquals(
            2 + (221 * 2),
            this.producerForTransactionPublishingOnExifOrImageTopic.history()
                .size());
        List<String> foundValue = this.producerForTransactionPublishingOnExifOrImageTopic.history()
            .stream()
            .filter((p) -> this.checkCopyrightOrArtist(p))
            .map((a) -> (ExchangedTiffData) a.value())
            .map((a) -> new String(a.getDataAsByte()))
            .collect(Collectors.toList());
        Assert.assertEquals(4, foundValue.size());
        Assert.assertThat(
            foundValue,
            IsIterableContainingInOrder.contains(
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER),
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER),
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER),
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER)));
    }

    private void waitForEndOfTransaction() {
        do {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ie) {
                Thread.currentThread()
                    .interrupt();
            }
        } while (!this.producerForTransactionPublishingOnExifOrImageTopic.transactionCommitted());
    }

    private boolean checkCopyrightOrArtist(ProducerRecord<String, HbaseData> p) {
        if (p.value() instanceof ExchangedTiffData) {
            short currentTag = ((ExchangedTiffData) p.value()).getTag();
            short[] path = ((ExchangedTiffData) p.value()).getPath();
            return ((currentTag == BeanProcessIncomingFile.EXIF_COPYRIGHT)
                && (Objects.deepEquals(path, BeanProcessIncomingFile.EXIF_COPYRIGHT_PATH)))
                || ((currentTag == BeanProcessIncomingFile.EXIF_ARTIST)
                    && (Objects.deepEquals(path, BeanProcessIncomingFile.EXIF_ARTIST_PATH)));
        }
        return false;
    }

    @Test
    public void test002_shouldSend120MsgsWhenParsingARAWFile() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
                0,
                0,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
                    .build()));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.updateBeginningOffsets(
                Collections.singletonMap(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0),
                    0L)));

        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.rebalance(
                Collections.singleton(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0))));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(() -> {
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(0));
        });
        this.waitForEndOfTransaction();
        Assert.assertEquals(
            1 + (221 * 1),
            this.producerForTransactionPublishingOnExifOrImageTopic.history()
                .size());

    }

    @Test
    public void test003_shouldUseMaxOfOffsetPlusOneCommitWhenSeveralRecordsAreUsed() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic-dup-filtered-file",
                0,
                2,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                0,
                6,
                "2",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                0,
                8,
                "3",
                FileToProcess.builder()
                    .withImageId("3")
                    .build()));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.updateBeginningOffsets(
                Collections.singletonMap(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0),
                    0L)));

        this.consumerForTopicWithFileToProcessValue.schedulePollTask(
            () -> this.consumerForTopicWithFileToProcessValue.rebalance(
                Collections.singleton(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicDupFilteredFile(), 0))));
        this.consumerForTopicWithFileToProcessValue.schedulePollTask(() -> {
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(0));
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(1));
            this.consumerForTopicWithFileToProcessValue.addRecord(asList.get(2));
        });

        this.waitForEndOfTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
            new TopicPartition(this.kafkaProperties.getTopics()
                .topicDupFilteredFile(), 0),
            new OffsetAndMetadata(9));

        final Map<TopicPartition, OffsetAndMetadata> actual = this.producerForTransactionPublishingOnExifOrImageTopic
            .consumerGroupOffsetsHistory()
            .get(0)
            .get("dummy.group.id");
        Assert.assertEquals(offsets, actual);
    }

    private boolean checkIfOptionalParametersArePresent(TiffFieldAndPath ifdFieldAndPath) {

        short currentTag = ifdFieldAndPath.getTiffField()
            .getTag()
            .getValue();
        if (currentTag == BeanProcessIncomingFile.EXIF_COPYRIGHT) {
            BeanProcessIncomingFile.LOGGER.debug("Found Exif copyright");
        }
        if (currentTag == BeanProcessIncomingFile.EXIF_ARTIST) {
            BeanProcessIncomingFile.LOGGER.debug("Found Exif artist");
        }
        return ((currentTag == BeanProcessIncomingFile.EXIF_COPYRIGHT)
            && (Objects.deepEquals(ifdFieldAndPath.getPath(), BeanProcessIncomingFile.EXIF_COPYRIGHT_PATH)))
            || ((currentTag == BeanProcessIncomingFile.EXIF_ARTIST)
                && (Objects.deepEquals(ifdFieldAndPath.getPath(), BeanProcessIncomingFile.EXIF_ARTIST_PATH)));
    }

}
