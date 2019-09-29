package com.gs.photo.workflow.consumers;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.photo.workflow.dao.GenericDAO;
import com.workflow.model.HbaseData;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

	private static Logger LOGGER = LogManager.getLogger(
		AbstractConsumerForRecordHbase.class);
	public static final int NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

	@Autowired
	protected GenericDAO genericDAO;
	protected ExecutorService[] services;

	@PostConstruct
	protected void init() {
		services = new ExecutorService[NB_OF_THREADS_TO_RECORD_IN_HBASE];
		for (int k = 0; k < NB_OF_THREADS_TO_RECORD_IN_HBASE; k++) {
			services[k] = Executors.newFixedThreadPool(
				1);
		}
	}

	protected final class DbTask implements Callable<Map<TopicPartition, OffsetAndMetadata>> {
		protected T[] data;
		protected TopicPartition partition;
		protected long lastOffset;
		private Class<T> cl;

		public DbTask(
				T[] data,
				TopicPartition partition,
				long offsetToCommit,
				Class<T> cl) {
			super();
			this.data = data;
			this.partition = partition;
			this.lastOffset = offsetToCommit + 1;
			this.cl = cl;
		}

		@Override
		public Map<TopicPartition, OffsetAndMetadata> call() throws Exception {
			genericDAO.put(
				data,
				cl);
			return Collections.singletonMap(
				partition,
				new OffsetAndMetadata(lastOffset + 1));
		}
	}

	protected void processMessagesFromTopic(Consumer<String, T> consumer, Class<T> cl) {
		List<Future<Map<TopicPartition, OffsetAndMetadata>>> futuresList = new ArrayList<>();
		while (true) {
			futuresList.clear();
			ConsumerRecords<String, T> records = consumer.poll(
				Long.MAX_VALUE);
			LOGGER.info(
				" found {} records ",
				records.count());
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, T>> partitionRecords = records.records(
					partition);
				List<T> foundRecords = new ArrayList<T>();
				for (ConsumerRecord<String, T> record : partitionRecords) {
					foundRecords.add(
						record.value());
				}
				T[] hit = (T[]) Array.newInstance(
					cl,
					foundRecords.size());
				foundRecords.toArray(
					hit);
				Future<Map<TopicPartition, OffsetAndMetadata>> f = services[partition.partition()
						% NB_OF_THREADS_TO_RECORD_IN_HBASE].submit(
							new DbTask(
								hit,
								partition,
								partitionRecords.get(
									partitionRecords.size() - 1).offset(),
								cl));
				futuresList.add(
					f);
			}
			futuresList.forEach(
				(f) -> {
					try {
						consumer.commitSync(
							f.get());
					} catch (CommitFailedException | InterruptedException | ExecutionException e) {
						LOGGER.warn(
							"Error whil commiting ",
							e);
					}
				});
		}
	}

}
