package com.gs.photo.workflow.streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.dupcheck.ApplicationConfig;
import com.gs.photo.workflow.dupcheck.IDuplicateCheck;
import com.gs.photo.workflow.dupcheck.config.IKafkaStreamProperties;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.gs.photos.serializers.FileToProcessSerializer;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ApplicationConfig.class)
@ActiveProfiles("test")
public class BeanDuplicateCheckTest {

    @Autowired
    protected IKafkaStreamProperties applicationSpecificProperties;

    @Autowired
    @MockBean
    public Void                      duplicateCheckInit;

    @Autowired
    @Qualifier("kafkaStreamTopologyProperties")
    protected Properties             kafkaStreamTopologyProperties;

    @Autowired
    protected IDuplicateCheck        beanDuplicateCheck;

    protected TopologyTestDriver     testDriver;

    @BeforeEach
    public void setUp() throws Exception {
        this.testDriver = new TopologyTestDriver(this.beanDuplicateCheck.buildKafkaStreamsTopology(),
            this.kafkaStreamTopologyProperties);
    }

    @Test
    public void shouldReceiveOnlyOneMessageOnFilteredTopicWhenOneMessageIsSent() {
        try {
            String id = UUID.randomUUID()
                .toString();

            TestInputTopic<String, FileToProcess> inputTopic = this.testDriver.createInputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicFileHashkey(),
                new StringSerializer(),
                new FileToProcessSerializer());
            TestOutputTopic<String, FileToProcess> outputTopicDeduplicated = this.testDriver.createOutputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicDupFilteredFile(),
                new StringDeserializer(),
                new FileToProcessDeserializer());
            TestOutputTopic<String, FileToProcess> outputTopicDuplicateFound = this.testDriver.createOutputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicDuplicateKeyImageFound(),
                new StringDeserializer(),
                new FileToProcessDeserializer());

            inputTopic.pipeInput(new TestRecord<String, FileToProcess>(id, this.buildFileToProcess(id)));
            Assert.assertEquals(false, outputTopicDeduplicated.isEmpty());
            KeyValue<String, FileToProcess> outPut = outputTopicDeduplicated.readKeyValue();
            Assert.assertEquals(id, outPut.key);
            Assert.assertEquals(true, outputTopicDuplicateFound.isEmpty());

        } catch (InvalidGroupIdException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Test
    public void shouldReceiveOnlyOneMessageOnFilteredTopicWhen2MessagesWithSameKeyAreSent() {
        try {
            String id = UUID.randomUUID()
                .toString();

            TestInputTopic<String, FileToProcess> inputTopic = this.testDriver.createInputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicFileHashkey(),
                new StringSerializer(),
                new FileToProcessSerializer());
            TestOutputTopic<String, FileToProcess> outputTopicDeduplicated = this.testDriver.createOutputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicDupFilteredFile(),
                new StringDeserializer(),
                new FileToProcessDeserializer());
            TestOutputTopic<String, FileToProcess> outputTopicDuplicateFound = this.testDriver.createOutputTopic(
                this.applicationSpecificProperties.getTopics()
                    .topicDuplicateKeyImageFound(),
                new StringDeserializer(),
                new FileToProcessDeserializer());

            inputTopic.pipeInput(new TestRecord<String, FileToProcess>(id, this.buildFileToProcess(id)));
            inputTopic.pipeInput(new TestRecord<String, FileToProcess>(id, this.buildFileToProcess(id)));
            Assert.assertEquals(false, outputTopicDeduplicated.isEmpty());
            KeyValue<String, FileToProcess> outPut = outputTopicDeduplicated.readKeyValue();
            Assert.assertEquals(true, outputTopicDeduplicated.isEmpty());
            Assert.assertEquals(id, outPut.key);
            Assert.assertEquals(false, outputTopicDuplicateFound.isEmpty());
            KeyValue<String, FileToProcess> outPutDuplicateFound = outputTopicDuplicateFound.readKeyValue();
            Assert.assertEquals("DUP-" + id, outPutDuplicateFound.key);

        } catch (InvalidGroupIdException e) {
            e.printStackTrace();
            throw e;
        }

    }

    private FileToProcess buildFileToProcess(String id) {
        return FileToProcess.builder()
            .withDataId("dataId")
            .withImageId(id)
            .withName("dataId")
            .withUrl("monurl")
            .withCompressedFile(false)
            .withIsLocal(false)
            .withImportEvent(
                ImportEvent.builder()
                    .withAlbum("MyAlbum")
                    .withImportName("Mon import")
                    .withKeyWords(Arrays.asList("kw1", "kw2"))
                    .withScanFolder("Scan folder")
                    .build())
            .build();
    }

}
