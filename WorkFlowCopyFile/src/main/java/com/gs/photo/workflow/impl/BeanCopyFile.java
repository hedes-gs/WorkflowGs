package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.ICopyFile;
import com.gs.photo.workflow.IServicesFile;

@Component
public class BeanCopyFile implements ICopyFile {

	protected static Logger LOGGER = LoggerFactory.getLogger(
		ICopyFile.class);

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@Value("${copy.repository}")
	protected String repository;

	@Value("${copy.group.id}")
	protected String copyGroupId;

	@Value("${topic.topicDupDilteredFile}")
	protected String topicDupDilteredFile;

	@Value("${topic.topicCopyOtherFile}")
	protected String topicFile;

	protected Path repositoryPath;

	@Autowired
	@Qualifier("consumerForTransactionalCopyForTopicWithStringKey")
	protected Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey;

	@Autowired
	@Qualifier("producerForPublishingInModeTransactionalOnStringTopic")
	protected Producer<String, String> producerForPublishingOnStringTopic;

	@Autowired
	protected IServicesFile beanServicesFile;

	@PostConstruct
	public void init() {
		repositoryPath = Paths.get(
			repository);
		beanTaskExecutor.execute(
			() -> processInputFile());
	}

	private Object processInputFile() {
		LOGGER.info(
			"Starting to process input messages from {} to {}",
			topicDupDilteredFile,
			topicFile);
		consumerForTransactionalCopyForTopicWithStringKey.subscribe(
			Collections.singleton(
				topicDupDilteredFile));
		producerForPublishingOnStringTopic.initTransactions();
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumerForTransactionalCopyForTopicWithStringKey.poll(
					250);
				long startTime = System.currentTimeMillis();
				producerForPublishingOnStringTopic.beginTransaction();
				LOGGER.debug(
					"Start transaction, for {}",
					records.count());
				records.forEach(
					(rec) -> {
						try {
							Path dest = copy(
								rec.key(),
								rec.value());
							Future<RecordMetadata> result = null;
							result = producerForPublishingOnStringTopic.send(
								new ProducerRecord<String, String>(
									topicFile,
									rec.key(),
									dest.toAbsolutePath().toString()));
							RecordMetadata data = result.get();
							LOGGER.info(
								"[EVENT][{}] Recorded data at [part={},offset={},topic={},time={}]",
								rec.key(),
								data.partition(),
								data.offset(),
								data.topic(),
								data.timestamp());
						} catch (IOException e) {
							LOGGER.error(
								"[EVENT][{}] Unexpected error",
								rec.key(),
								e);
						} catch (InterruptedException e) {
							LOGGER.error(
								"[EVENT][{}] Unexpected error",
								rec.key(),
								e);
						} catch (ExecutionException e) {
							LOGGER.error(
								"[EVENT][{}] Unexpected error",
								rec.key(),
								e);
						}
					});
				producerForPublishingOnStringTopic.sendOffsetsToTransaction(
					currentOffsets(
						consumerForTransactionalCopyForTopicWithStringKey,
						records),
					copyGroupId);
				producerForPublishingOnStringTopic.commitTransaction();
				LOGGER.debug(
					"[EVENT][{}]  Commit transaction, time: {}, nb of records : {}",
					(System.currentTimeMillis() - startTime) / 1000.0,
					records.count());
			} catch (Exception e) {
				producerForPublishingOnStringTopic.abortTransaction();
				LOGGER.error(
					"Unexpected error",
					e);
			}
		}
	}

	private Map<TopicPartition, OffsetAndMetadata> currentOffsets(
			Consumer<String, String> consumerTransactionalForTopicWithStringKey2,
			ConsumerRecords<String, String> records) {
		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
		for (TopicPartition partition : records.partitions()) {
			List<ConsumerRecord<String, String>> partitionedRecords = records.records(
				partition);
			long offset = partitionedRecords.get(
				partitionedRecords.size() - 1).offset();
			offsetsToCommit.put(
				partition,
				new OffsetAndMetadata(offset + 1));
		}
		return offsetsToCommit;
	}

	public Path copy(String key, String value) throws IOException {
		Path sourcePath = Paths.get(
			value);
		key = key + "--" + sourcePath.getFileName();
		String currentFolder = beanServicesFile.getCurrentFolderInWhichCopyShouldBeDone(
			repositoryPath);

		Path destPath = repositoryPath.resolve(
			Paths.get(
				currentFolder,
				key));

		try {
			LOGGER.info(
				"Copying input file from {} to {}",
				sourcePath.toAbsolutePath(),
				destPath.toAbsolutePath());
			if (!Files.exists(
				destPath)) {
				Files.copy(
					sourcePath,
					destPath);
				return destPath;
			}
			throw new IOException("Erreur : destination path " + destPath + " Already exists");
		} catch (IOException e) {
			LOGGER.error(
				"Unexpected error whyle copying from  {} to  {}",
				sourcePath,
				destPath,
				e);
			throw e;
		}
	}

}
