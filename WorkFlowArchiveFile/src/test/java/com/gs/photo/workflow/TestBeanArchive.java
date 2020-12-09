package com.gs.photo.workflow;

import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.impl.BeanArchive;
import com.gs.photo.workflow.impl.FileUtils;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = { ApplicationConfig.class, BeanArchive.class })
public class TestBeanArchive {

    @MockBean
    @Qualifier("consumerForTransactionalReadOfFileToProcess")
    protected Consumer<String, FileToProcess> consumerForTransactionalReadOfFileToProcess;

    @MockBean
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    protected IBeanArchive                    beanArchive;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @MockBean
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents>  producerForPublishingWfEvents;

    @MockBean
    @Qualifier("hdfsFileSystem")
    protected FileSystem                  hdfsFileSystem;

    @MockBean
    @Qualifier("userGroupInformationAction")
    protected IUserGroupInformationAction userGroupInformationAction;

    @MockBean
    @Qualifier("fileUtils")
    protected FileUtils                   fileUtils;

    protected class ExceptionToStop extends RuntimeException {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        Mockito.doAnswer(invocation -> {
            Runnable arg = (Runnable) invocation.getArgument(0);
            arg.run();
            return null;
        })
            .when(this.beanTaskExecutor)
            .execute((Runnable) ArgumentMatchers.any());
        Mockito.doAnswer(invocation -> {
            PrivilegedAction<?> arg = (PrivilegedAction<?>) invocation.getArgument(0);
            if (arg != null) { return arg.run(); }
            return null;
        })
            .when(this.userGroupInformationAction)
            .run((PrivilegedAction<?>) ArgumentMatchers.any());
    }

    @Test
    public void test001_shouldSendOneWfEventWhenReceivingOneRecord() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .withDataId("<dataId>")
                    .withHost("localhost")
                    .withName("file")
                    .withPath("/")
                    .withImportEvent(
                        ImportEvent.builder()
                            .withImportName("import test")
                            .build())
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.when(this.consumerForTransactionalReadOfFileToProcess.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionToStop());

        Mockito.when(this.hdfsFileSystem.mkdirs(ArgumentMatchers.any()))
            .thenReturn(true);

        try {
            this.beanArchive.init();
        } catch (ExceptionToStop e) {
        }
        Mockito.verify(this.producerForPublishingWfEvents, Mockito.times(1))
            .send(ArgumentMatchers.any());
    }

    @Test
    public void test002_shouldCreateOneDirWhenReceivingOneRecord() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .withDataId("<dataId>")
                    .withHost("localhost")
                    .withName("file")
                    .withPath("/")
                    .withImportEvent(
                        ImportEvent.builder()
                            .withImportName("import test")
                            .build())
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.when(this.consumerForTransactionalReadOfFileToProcess.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionToStop());
        Mockito.when(this.hdfsFileSystem.mkdirs(ArgumentMatchers.any()))
            .thenReturn(true);

        try {
            this.beanArchive.init();
        } catch (ExceptionToStop e) {
        }
        Mockito.verify(this.hdfsFileSystem, Mockito.times(1))
            .mkdirs((Path) ArgumentMatchers.any());
    }

    @Test
    public void test003_shouldCopyLocalToRemoteWhenReceivingOneRecord() throws IOException {
        Map<TopicPartition, List<ConsumerRecord<String, FileToProcess>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, FileToProcess>> asList = Arrays.asList(
            new ConsumerRecord<>("topic",
                1,
                0,
                "1",
                FileToProcess.builder()
                    .withDataId("<dataId>")
                    .withHost("localhost")
                    .withName("file")
                    .withPath("/")
                    .withImportEvent(
                        ImportEvent.builder()
                            .withImportName("import test")
                            .build())
                    .build()));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, FileToProcess> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.when(this.consumerForTransactionalReadOfFileToProcess.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenThrow(new ExceptionToStop());
        Mockito.when(this.hdfsFileSystem.mkdirs(ArgumentMatchers.any()))
            .thenReturn(true);

        try {
            this.beanArchive.init();
        } catch (ExceptionToStop e) {
        }
        Mockito.verify(this.fileUtils, Mockito.times(1))
            .copyRemoteToLocal(
                (FileToProcess) ArgumentMatchers.any(),
                (OutputStream) ArgumentMatchers.any(),
                (Integer) ArgumentMatchers.any());
    }

}
