package com.gs.photo.workflow.extimginfo.impl;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.IIgniteDAO;
import com.gs.photo.workflow.extimginfo.IAccessDirectlyFile;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.WorkflowExtractImageInfo;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.files.FileToProcess;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@SpringBootTest(classes = { WorkflowExtractImageInfo.class })
class BeanProcessIncomingFileTest {
    protected static Logger                             LOGGER = LoggerFactory
        .getLogger(BeanProcessIncomingFileTest.class);

    @MockBean
    private Ignite                                      ignite;

    @MockBean
    protected IBeanTaskExecutor                         beanTaskExecutor;

    @MockBean
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue;

    @MockBean
    protected Supplier<Producer<String, HbaseData>>     producerSupplierForTransactionPublishingOnExifTopic;

    @Mock
    protected Consumer<String, FileToProcess>           consumerForTopicWithFileToProcessValue;

    @Mock
    protected Producer<String, HbaseData>               producerForTransactionPublishingOnExifOrImageTopic;

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

    protected class LocalFuture<HbaseData> implements Future<HbaseData> {

        @Override
        public boolean isCancelled() { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isDone() { // TODO Auto-generated method stub
            return true;
        }

        @Override
        public HbaseData get() throws InterruptedException, ExecutionException { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public HbaseData get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> addListener(GenericFutureListener<? extends Future<? super HbaseData>> arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> addListeners(GenericFutureListener<? extends Future<? super HbaseData>>... arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> await() throws InterruptedException { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean await(long arg0) throws InterruptedException { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean await(long arg0, TimeUnit arg1) throws InterruptedException { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Future<HbaseData> awaitUninterruptibly() { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean awaitUninterruptibly(long arg0) { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long arg0, TimeUnit arg1) { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean cancel(boolean arg0) { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Throwable cause() { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public HbaseData getNow() { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isCancellable() { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isSuccess() { // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Future<HbaseData> removeListener(GenericFutureListener<? extends Future<? super HbaseData>> arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> removeListeners(GenericFutureListener<? extends Future<? super HbaseData>>... arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> sync() throws InterruptedException { // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Future<HbaseData> syncUninterruptibly() { // TODO Auto-generated method stub
            return null;
        }

    }

    @Captor
    protected ArgumentCaptor<ProducerRecord<String, HbaseData>> valueCaptor;

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

    @BeforeEach
    public void setUp() throws Exception {
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.isReady())
            .thenReturn(true);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
        Mockito.when(this.iIgniteDAO.get("2"))
            .thenReturn(Optional.ofNullable(null));
        Mockito.when(this.producerForTransactionPublishingOnExifOrImageTopic.send(ArgumentMatchers.any()))
            .thenReturn(new LocalFuture<>());
        Mockito.when(this.kafkaConsumerFactoryForFileToProcessValue.get())
            .thenReturn(this.consumerForTopicWithFileToProcessValue);
        Mockito.when(this.producerSupplierForTransactionPublishingOnExifTopic.get())
            .thenReturn(this.producerForTransactionPublishingOnExifOrImageTopic);

    }

    @Test
    public void test001_shouldRetrieveNoCopyrightAndArtist() throws IOException {
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
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
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic",
                1,
                0,
                "2",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()),
            new ConsumerRecord<>("topic",
                1,
                0,
                "2",
                FileToProcess.builder()
                    .withImageId("2")
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);
        Mockito.when(this.accessDirectlyFile.readFirstBytesOfFileRetry(ArgumentMatchers.any()))
            .thenReturn(Optional.of(bb.array()));
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
        Mockito.when(this.producerForTransactionPublishingOnExifOrImageTopic.send(ArgumentMatchers.any()))
            .thenReturn(new LocalFuture<>());
        try {
            this.beanProcessIncomingFile.init();
        } catch (ExceptionEndOfTest e) {
        }

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ie) {
            Thread.currentThread()
                .interrupt();
        }
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(1))
            .send(this.valueCaptor.capture());
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(221 * 2))
            .send(this.valueCaptor.capture(), ArgumentMatchers.any());
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
                    .withImageId("1")
                    .build()),
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
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
        Mockito.when(this.producerForTransactionPublishingOnExifOrImageTopic.send(ArgumentMatchers.any()))
            .thenReturn(new LocalFuture<>());
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
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(221 * 2))
            .send(this.valueCaptor.capture(), ArgumentMatchers.any());
        List<String> foundValue = this.valueCaptor.getAllValues()
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
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
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
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(221))
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
                    .withImageId("1")
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                1,
                6,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
                    .build()),
            new ConsumerRecord<>("topic-dup-filtered-file",
                1,
                8,
                "1",
                FileToProcess.builder()
                    .withImageId("1")
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
            .sendOffsetsToTransaction(ArgumentMatchers.eq(offsets), (ConsumerGroupMetadata) ArgumentMatchers.any());

    }

    private boolean checkIfOptionalParametersArePresent(TiffFieldAndPath ifdFieldAndPath) {

        short currentTag = ifdFieldAndPath.getTiffField()
            .getTag()
            .getValue();
        BeanProcessIncomingFileTest.LOGGER.info(
            "... info tag : {}, copyright {} ",
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
