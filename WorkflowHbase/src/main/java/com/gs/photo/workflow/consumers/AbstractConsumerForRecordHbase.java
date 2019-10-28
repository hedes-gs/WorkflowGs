package com.gs.photo.workflow.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.gs.photo.workflow.dao.GenericDAO;
import com.workflow.model.HbaseData;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

	private static Logger            LOGGER                           = LogManager
			.getLogger(AbstractConsumerForRecordHbase.class);
	public static final int          NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

	@Autowired
	protected GenericDAO<T>          genericDAO;

	@Autowired
	protected Producer<String, Long> producerForPublishingInModeTransactionalOnLongTopic;

	@Value("${topic.topicRecordedCountObjects}")
	protected String                 topicRecordedCountObjects;

	protected ExecutorService[]      services;

	@Value("${group.id}")
	private String                   groupId;

	@PostConstruct
	protected void init() {
		this.services = new ExecutorService[AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE];
		for (
				int k = 0;
				k < AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE;
				k++) {
			this.services[k] = Executors.newFixedThreadPool(1);
		}
	}

	protected static final class DbTaskResult {
		protected final TopicPartition    partition;
		protected final OffsetAndMetadata commitData;
		protected final Map<String, Long> keysNumber;

		public TopicPartition getPartition() {
			return this.partition;
		}

		public OffsetAndMetadata getCommitData() {
			return this.commitData;
		}

		public Map<String, Long> getKeysNumber() {
			return this.keysNumber;
		}

		protected DbTaskResult(
				TopicPartition partition,
				OffsetAndMetadata commitData,
				Map<String, Long> keysNumber) {
			super();
			this.partition = partition;
			this.commitData = commitData;
			this.keysNumber = keysNumber;
		}

	}

	protected final class DbTask implements Callable<DbTaskResult> {
		protected ListMultimap<String, T> data;
		protected TopicPartition          partition;
		protected long                    lastOffset;

		public DbTask(
				ListMultimap<String, T> data,
				TopicPartition partition,
				long offsetToCommit) {
			super();
			this.data = data;
			this.partition = partition;
			this.lastOffset = offsetToCommit + 1;
		}

		@Override
		public DbTaskResult call() throws Exception {
			Map<String, Long> keysNumber = this.data.keys().stream().collect(Collectors.groupingBy(Function.identity(),
					Collectors.counting()));
			this.data.values().stream().collect(Collectors.groupingBy((k) -> k.getClass())).forEach((k, v) -> {
				AbstractConsumerForRecordHbase.this.genericDAO.put(v,
						(Class<T>) k);
			});
			return new DbTaskResult(this.partition, new OffsetAndMetadata(this.lastOffset + 1), keysNumber);
		}
	}

	protected void processMessagesFromTopic(Consumer<String, T> consumer) {
		List<Future<DbTaskResult>> futuresList = new ArrayList<>();
		this.producerForPublishingInModeTransactionalOnLongTopic.initTransactions();
		while (true) {
			futuresList.clear();
			ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
			this.producerForPublishingInModeTransactionalOnLongTopic.beginTransaction();
			AbstractConsumerForRecordHbase.LOGGER.info(" found {} records ",
					records.count());
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, T>> partitionRecords = records.records(partition);
				ListMultimap<String, T> multimapRecords = MultimapBuilder.treeKeys().arrayListValues().build();
				for (ConsumerRecord<String, T> record : partitionRecords) {
					multimapRecords.put(record.key(),
							record.value());
				}
				Future<DbTaskResult> f = this.services[partition.partition()
						% AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE]
								.submit(new DbTask(
									multimapRecords,
									partition,
									partitionRecords.get(partitionRecords.size() - 1).offset()));
				futuresList.add(f);
			}
			Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

			futuresList.forEach((f) -> {
				try {
					DbTaskResult task = f.get();
					offsets.put(task.getPartition(),
							task.getCommitData());
					task.keysNumber.forEach((k, v) -> {
						ProducerRecord<String, Long> producerRecord = new ProducerRecord<>(
							this.topicRecordedCountObjects,
							k,
							v);
						this.producerForPublishingInModeTransactionalOnLongTopic.send(producerRecord);
					});

				} catch (
						CommitFailedException |
						InterruptedException |
						ExecutionException e) {
					AbstractConsumerForRecordHbase.LOGGER.warn("Error whil commiting ",
							e);
				}
			});
			this.producerForPublishingInModeTransactionalOnLongTopic.sendOffsetsToTransaction(offsets,
					this.groupId);
			this.producerForPublishingInModeTransactionalOnLongTopic.commitTransaction();

		}
	}

}
