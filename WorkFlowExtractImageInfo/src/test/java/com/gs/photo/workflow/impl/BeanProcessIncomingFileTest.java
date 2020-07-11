package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
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
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.exif.ExifServiceImpl;
import com.gs.photos.workflow.metadata.NameServiceTestConfiguration;
import com.workflow.model.files.FileToProcess;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        ExifServiceImpl.class, NameServiceTestConfiguration.class, BeanFileMetadataExtractor.class,
        BeanProcessIncomingFile.class, ApplicationConfig.class })
public class BeanProcessIncomingFileTest {
    @MockBean
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    @Qualifier("consumerForTopicWithFileToProcessValue")
    @MockBean
    protected Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue;

    @Autowired
    @Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
    @MockBean
    protected Producer<String, Object>        producerForTransactionPublishingOnExifOrImageTopic;

    @Autowired
    protected IIgniteDAO                      iIgniteDAO;

    @Autowired
    protected BeanProcessIncomingFile         beanProcessIncomingFile;

    protected static class ExceptionEndOfTest extends RuntimeException {

    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
        FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
        fc.read(bb);
        Mockito.when(this.iIgniteDAO.get("1"))
            .thenReturn(Optional.of(bb.array()));
    }

    @Test
    public void test001_shouldSend118MsgsWhenParsingARAWFile() throws IOException {
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
        Mockito.verify(this.producerForTransactionPublishingOnExifOrImageTopic, Mockito.times(118))
            .send(ArgumentMatchers.any());

    }

    @Test
    public void test002_shouldUseMaxOfOffsetPlusOneCommitWhenSeveralRecordsAreUsed() throws IOException {
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
            .sendOffsetsToTransaction(ArgumentMatchers.eq(offsets), ArgumentMatchers.any());

    }

}
