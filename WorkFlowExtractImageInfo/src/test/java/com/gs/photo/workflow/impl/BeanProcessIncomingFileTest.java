package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.ApplicationConfig;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.exif.ExifServiceImpl;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.NameServiceTestConfiguration;
import com.gs.photos.workflow.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.files.FileToProcess;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        ExifServiceImpl.class, NameServiceTestConfiguration.class, BeanFileMetadataExtractor.class,
        BeanProcessIncomingFile.class, ApplicationConfig.class })
public class BeanProcessIncomingFileTest {
    @MockBean
    protected IBeanTaskExecutor                              beanTaskExecutor;

    @Autowired
    @Qualifier("consumerForTopicWithFileToProcessValue")
    @MockBean
    protected Consumer<String, FileToProcess>                consumerForTopicWithFileToProcessValue;

    @Autowired
    @Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
    @MockBean
    protected Producer<String, Object>                       producerForTransactionPublishingOnExifOrImageTopic;

    @Autowired
    @MockBean
    protected IIgniteDAO                                     iIgniteDAO;

    @Autowired
    protected BeanProcessIncomingFile                        beanProcessIncomingFile;

    @Autowired
    protected IFileMetadataExtractor                         beanFileMetadataExtractor;

    @Captor
    protected ArgumentCaptor<ProducerRecord<String, Object>> valueCaptor;

    protected static class ExceptionEndOfTest extends RuntimeException {

        public ExceptionEndOfTest() { this(new InterruptedException()); }

        public ExceptionEndOfTest(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace
        ) {
            super(message,
                cause,
                enableSuppression,
                writableStackTrace);
        }

        public ExceptionEndOfTest(
            String message,
            Throwable cause
        ) { super(message,
            cause); }

        public ExceptionEndOfTest(String message) { super(message); }

        public ExceptionEndOfTest(Throwable cause) { super(cause); }

    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.isReady())
            .thenReturn(true);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
    }

    @Test
    public void test001_shouldRetrieveNoCopyrightAndArtist() throws IOException {
        final Collection<IFD> IFDs = this.beanFileMetadataExtractor.readIFDs("1")
            .get();

        List<TiffFieldAndPath> optionalParameters = IFD.tiffFieldsAsStream(IFDs.stream())
            .filter((ifd) -> this.checkIfOptionalParametersArePresent(ifd))
            .collect(Collectors.toList());
        Assert.assertEquals(0, optionalParameters.size());
    }

    @Test
    public void test001_shouldRetrieveDefaultCopyrightAndArtist() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.doAnswer(invocation -> {
            Runnable arg = (Runnable) invocation.getArgument(0);
            arg.run();
            return null;
        })
            .when(this.beanTaskExecutor)
            .execute((Runnable) ArgumentMatchers.any());
        Mockito.when(this.consumerForTopicWithFileToProcessValue.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionEndOfTest());
        try {
            this.beanProcessIncomingFile.init();
        } catch (ExceptionEndOfTest e) {
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException ie) {
            Thread.currentThread()
                .interrupt();
        }
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(1))
            .send(this.valueCaptor.capture());
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(119))
            .send(this.valueCaptor.capture(), ArgumentMatchers.any());
        List<String> foundValue = this.valueCaptor.getAllValues()
            .stream()
            .filter((p) -> this.checkCopyrightOrArtist(p))
            .map((a) -> (ExchangedTiffData) a.value())
            .map((a) -> new String(a.getDataAsByte()))
            .collect(Collectors.toList());
        Assert.assertEquals(2, foundValue.size());
        Assert.assertThat(
            foundValue,
            IsIterableContainingInOrder.contains(
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER),
                new String(BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER)));
    }

    private boolean checkCopyrightOrArtist(ProducerRecord<String, Object> p) {
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
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.doAnswer(invocation -> {
            Runnable arg = (Runnable) invocation.getArgument(0);
            arg.run();
            return null;
        })
            .when(this.beanTaskExecutor)
            .execute((Runnable) ArgumentMatchers.any());
        Mockito.when(this.consumerForTopicWithFileToProcessValue.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionEndOfTest());
        try {
            this.beanProcessIncomingFile.init();
        } catch (ExceptionEndOfTest e) {
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException ie) {
            Thread.currentThread()
                .interrupt();
        }
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(1))
            .send(ArgumentMatchers.any());
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(119))
            .send(ArgumentMatchers.any(), ArgumentMatchers.any());

    }

    @Test
    public void test003_shouldUseMaxOfOffsetPlusOneCommitWhenSeveralRecordsAreUsed() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic-dup-filtered-file",
                1,
                2,
                "1",
                FileToProcess.builder()
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                1,
                6,
                "1",
                FileToProcess.builder()
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                1,
                8,
                "1",
                FileToProcess.builder()
                    .build()));
        mapOfRecords.put(new TopicPartition("topic-dup-filtered-file", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.doAnswer(invocation -> {
            Runnable arg = (Runnable) invocation.getArgument(0);
            arg.run();
            return null;
        })
            .when(this.beanTaskExecutor)
            .execute((Runnable) ArgumentMatchers.any());
        Mockito.when(this.consumerForTopicWithFileToProcessValue.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionEndOfTest());
        try {
            this.beanProcessIncomingFile.init();
        } catch (ExceptionEndOfTest e) {
        }

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException ie) {
            Thread.currentThread()
                .interrupt();
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic-dup-filtered-file", 1), new OffsetAndMetadata(9));

        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic)
            .sendOffsetsToTransaction(ArgumentMatchers.eq(offsets), (String) ArgumentMatchers.any());

    }

    private boolean checkIfOptionalParametersArePresent(TiffFieldAndPath ifdFieldAndPath) {

        short currentTag = ifdFieldAndPath.getTiffField()
            .getTag()
            .getValue();
        BeanProcessIncomingFile.LOGGER.info(
            "... info tag : {}, copyroght {} ",
            Integer.toHexString(currentTag),
            Integer.toHexString(BeanProcessIncomingFile.EXIF_COPYRIGHT));
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
