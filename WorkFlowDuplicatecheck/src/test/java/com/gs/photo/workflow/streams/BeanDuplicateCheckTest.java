package com.gs.photo.workflow.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.IDuplicateCheck;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BeanDuplicateCheckTest {

	@Value("${topic.pathNameTopic}")
	protected String pathNameTopic;
	@Value("${bootstrap.servers}")
	protected String bootstrapServers;
	@Value("${topic.duplicateImageFoundTopic}")
	protected String duplicateImageFoundTopic;
	@Value("${topic.uniqueImageFoundTopic}")
	protected String uniqueImageFoundTopic;

	@Autowired
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	protected Producer<String, String> producerForPublishingOnImageTopic;

	@Autowired
	protected IDuplicateCheck beanDuplicateCheck;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	@Ignore
	public void shouldReceiveOnlyOneMessageOnFilteredTopicWhenOneMessageIsSent() {
		try {
			KafkaStreams kafkaStreams = beanDuplicateCheck.buildKafkaStreamsTopology();
			kafkaStreams.start();
			String id = UUID.randomUUID().toString();
			producerForPublishingOnImageTopic.send(
				new ProducerRecord<String, String>(pathNameTopic, id, "3"));
			producerForPublishingOnImageTopic.flush();

			consumerForTopicWithStringKey.subscribe(
				Arrays.asList(
					duplicateImageFoundTopic,
					uniqueImageFoundTopic));
			ConsumerRecords<String, String> records = consumerForTopicWithStringKey.poll(
				10000);
			assertNotNull(
				records);
			assertEquals(
				1,
				records.count());
			String key = null;
			for (ConsumerRecord<String, String> cr : records.records(
				uniqueImageFoundTopic)) {
				key = cr.key();
			}
			assertEquals(
				id,
				key);

		} catch (InvalidGroupIdException e) {
			e.printStackTrace();
			throw e;
		}

	}

	@Test
	@Ignore

	public void shouldReceiveOnlyOneMessageOnDuplicateTopicWhen2MessagesAreSent() {
		KafkaStreams kafkaStreams = beanDuplicateCheck.buildKafkaStreamsTopology();
		kafkaStreams.start();
		String id = UUID.randomUUID().toString();
		producerForPublishingOnImageTopic.send(
			new ProducerRecord<String, String>(pathNameTopic, id, "3"));
		producerForPublishingOnImageTopic.send(
			new ProducerRecord<String, String>(pathNameTopic, id, "4"));
		producerForPublishingOnImageTopic.flush();

		consumerForTopicWithStringKey.subscribe(
			Arrays.asList(
				duplicateImageFoundTopic,
				uniqueImageFoundTopic));
		int nbOfFoundUniqueKey = 0;
		int nbOfFoundDuplicateKey = 0;
		while (true) {
			ConsumerRecords<String, String> records = consumerForTopicWithStringKey.poll(
				10000);

			if (records != null && records.count() > 0) {
				for (ConsumerRecord<String, String> cr : records.records(
					uniqueImageFoundTopic)) {
					if (id.equals(
						cr.key())) {
						nbOfFoundUniqueKey++;
					}
				}
				for (ConsumerRecord<String, String> cr : records.records(
					duplicateImageFoundTopic)) {
					if (("DUP-" + id).equals(
						cr.key())) {
						nbOfFoundDuplicateKey++;
					}
				}
				consumerForTopicWithStringKey.commitSync();

			} else {
				break;
			}

		}
		assertEquals(
			1,
			nbOfFoundDuplicateKey);
		assertEquals(
			1,
			nbOfFoundUniqueKey);
	}

	@Test
	@Ignore

	public void shouldReceiveOnlyOneMessageOnFilteredTopicWhen2MessagesWithSameKeyAreSent() {
		try {
			KafkaStreams kafkaStreams = beanDuplicateCheck.buildKafkaStreamsTopology();
			kafkaStreams.start();
			String id = UUID.randomUUID().toString();
			producerForPublishingOnImageTopic.send(
				new ProducerRecord<String, String>(pathNameTopic, id, "3"));
			producerForPublishingOnImageTopic.send(
				new ProducerRecord<String, String>(pathNameTopic, id, "4"));
			producerForPublishingOnImageTopic.flush();

			consumerForTopicWithStringKey.subscribe(
				Arrays.asList(
					duplicateImageFoundTopic,
					uniqueImageFoundTopic));
			do {
				ConsumerRecords<String, String> record = consumerForTopicWithStringKey.poll(
					10000);
				if (record != null && record.count() > 0) {
					record.forEach(
						(c) -> System.err.println(
							"Receive " + c.key() + " on topic " + c.topic()));
					consumerForTopicWithStringKey.commitSync();

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
