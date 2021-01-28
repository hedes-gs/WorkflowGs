package com.gs.photo.workflow.streams;

import java.util.Arrays;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.dupcheck.IDuplicateCheck;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BeanDuplicateCheckTest {

    @Value("${topic.pathNameTopic}")
    protected String                   pathNameTopic;
    @Value("${bootstrap.servers}")
    protected String                   bootstrapServers;
    @Value("${topic.topicDuplicateKeyImageFound}")
    protected String                   duplicateImageFoundTopic;
    @Value("${topic.uniqueImageFoundTopic}")
    protected String                   uniqueImageFoundTopic;

    @Autowired
    protected Consumer<String, String> consumerForTopicWithStringKey;

    @Autowired
    protected Producer<String, String> producerForPublishingOnImageTopic;

    @Autowired
    protected IDuplicateCheck          beanDuplicateCheck;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @Test
    @Ignore
    public void shouldReceiveOnlyOneMessageOnFilteredTopicWhenOneMessageIsSent() {
        try {
            KafkaStreams kafkaStreams = this.beanDuplicateCheck.buildKafkaStreamsTopology();
            kafkaStreams.start();
            String id = UUID.randomUUID()
                .toString();
            this.producerForPublishingOnImageTopic
                .send(new ProducerRecord<String, String>(this.pathNameTopic, id, "3"));
            this.producerForPublishingOnImageTopic.flush();

            this.consumerForTopicWithStringKey
                .subscribe(Arrays.asList(this.duplicateImageFoundTopic, this.uniqueImageFoundTopic));
            ConsumerRecords<String, String> records = this.consumerForTopicWithStringKey.poll(10000);
            Assert.assertNotNull(records);
            Assert.assertEquals(1, records.count());
            String key = null;
            for (ConsumerRecord<String, String> cr : records.records(this.uniqueImageFoundTopic)) {
                key = cr.key();
            }
            Assert.assertEquals(id, key);

        } catch (InvalidGroupIdException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Test
    @Ignore

    public void shouldReceiveOnlyOneMessageOnDuplicateTopicWhen2MessagesAreSent() {
        KafkaStreams kafkaStreams = this.beanDuplicateCheck.buildKafkaStreamsTopology();
        kafkaStreams.start();
        String id = UUID.randomUUID()
            .toString();
        this.producerForPublishingOnImageTopic.send(new ProducerRecord<String, String>(this.pathNameTopic, id, "3"));
        this.producerForPublishingOnImageTopic.send(new ProducerRecord<String, String>(this.pathNameTopic, id, "4"));
        this.producerForPublishingOnImageTopic.flush();

        this.consumerForTopicWithStringKey
            .subscribe(Arrays.asList(this.duplicateImageFoundTopic, this.uniqueImageFoundTopic));
        int nbOfFoundUniqueKey = 0;
        int nbOfFoundDuplicateKey = 0;
        while (true) {
            ConsumerRecords<String, String> records = this.consumerForTopicWithStringKey.poll(10000);

            if ((records != null) && (records.count() > 0)) {
                for (ConsumerRecord<String, String> cr : records.records(this.uniqueImageFoundTopic)) {
                    if (id.equals(cr.key())) {
                        nbOfFoundUniqueKey++;
                    }
                }
                for (ConsumerRecord<String, String> cr : records.records(this.duplicateImageFoundTopic)) {
                    if (("DUP-" + id).equals(cr.key())) {
                        nbOfFoundDuplicateKey++;
                    }
                }
                this.consumerForTopicWithStringKey.commitSync();

            } else {
                break;
            }

        }
        Assert.assertEquals(1, nbOfFoundDuplicateKey);
        Assert.assertEquals(1, nbOfFoundUniqueKey);
    }

    @Test
    @Ignore

    public void shouldReceiveOnlyOneMessageOnFilteredTopicWhen2MessagesWithSameKeyAreSent() {
        try {
            KafkaStreams kafkaStreams = this.beanDuplicateCheck.buildKafkaStreamsTopology();
            kafkaStreams.start();
            String id = UUID.randomUUID()
                .toString();
            this.producerForPublishingOnImageTopic
                .send(new ProducerRecord<String, String>(this.pathNameTopic, id, "3"));
            this.producerForPublishingOnImageTopic
                .send(new ProducerRecord<String, String>(this.pathNameTopic, id, "4"));
            this.producerForPublishingOnImageTopic.flush();

            this.consumerForTopicWithStringKey
                .subscribe(Arrays.asList(this.duplicateImageFoundTopic, this.uniqueImageFoundTopic));
            do {
                ConsumerRecords<String, String> record = this.consumerForTopicWithStringKey.poll(10000);
                if ((record != null) && (record.count() > 0)) {
                    record.forEach((c) -> System.err.println("Receive " + c.key() + " on topic " + c.topic()));
                    this.consumerForTopicWithStringKey.commitSync();

                } else {
                    break;
                }
            } while (true);
        } catch (InvalidGroupIdException e) {
            e.printStackTrace();
            throw e;
        }

    }

}
