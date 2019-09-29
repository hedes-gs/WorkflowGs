package com.gs.photo.workflow.impl;

import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IProcessIncomingFiles;
import com.gs.photos.workflow.metadata.IFD;
import com.workflow.model.ExchangedTiffData;

@Component
public class BeanProcessIncomingFile implements IProcessIncomingFiles {

	public static final int NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

	protected static Logger LOGGER = LoggerFactory.getLogger(
		IProcessIncomingFiles.class);
	protected ExecutorService[] services;

	protected final class DbTask implements Callable<Map<TopicPartition, OffsetAndMetadata>> {
		protected ConsumerRecord<String, String>[] records;
		protected TopicPartition partition;
		protected long lastOffset;

		public DbTask(
				ConsumerRecord<String, String>[] records,
				TopicPartition partition,
				long offsetToCommit) {
			super();
			this.records = records;
			this.partition = partition;
			this.lastOffset = offsetToCommit + 1;
		}

		@Override
		public Map<TopicPartition, OffsetAndMetadata> call() throws Exception {
			for (ConsumerRecord<String, String> record : records) {
				processIncomingRecord(
					record);
			}

			return Collections.singletonMap(
				partition,
				new OffsetAndMetadata(lastOffset + 1));
		}
	}

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@Value("${topic.topicCopyRawFile}")
	protected String topicCopyFile;

	@Value("${topic.inputExifTopic}")
	protected String topicExif;

	@Value("${topic.recordThumbTopic}")
	protected String topicJpegFiles;

	@Autowired
	protected IFileMetadataExtractor beanFileMetadataExtractor;

	@Autowired
	@Qualifier("consumerForTopicWithStringKey")
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	@Qualifier("producerForPublishingOnExifTopic")
	protected Producer<String, ExchangedTiffData> producerForPublishingOnExifTopic;

	@Autowired
	@Qualifier("producerForPublishingOnJpegImageTopic")
	protected Producer<String, byte[]> producerForPublishingOnJpegImageTopic;

	@PostConstruct
	public void init() {
		services = new ExecutorService[NB_OF_THREADS_TO_RECORD_IN_HBASE];
		for (int k = 0; k < NB_OF_THREADS_TO_RECORD_IN_HBASE; k++) {
			services[k] = Executors.newFixedThreadPool(
				1);
		}
		beanTaskExecutor.execute(
			() -> processInputFile());
	}

	protected void processInputFile() {
		consumerForTopicWithStringKey.subscribe(
			Collections.singleton(
				topicCopyFile));
		LOGGER.info(
			"Starting process input file...");
		List<Future<Map<TopicPartition, OffsetAndMetadata>>> futuresList = new ArrayList<>();

		while (true) {
			try {
				futuresList.clear();
				ConsumerRecords<String, String> records = consumerForTopicWithStringKey.poll(
					500);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(
						partition);
					List<ConsumerRecord<String, String>> foundRecords = new ArrayList<>();
					for (ConsumerRecord<String, String> record : partitionRecords) {
						foundRecords.add(
							record);
					}
					ConsumerRecord<String, String>[] hit = (ConsumerRecord<String, String>[]) Array.newInstance(
						ConsumerRecord.class,
						foundRecords.size());
					foundRecords.toArray(
						hit);
					Future<Map<TopicPartition, OffsetAndMetadata>> f = services[partition.partition()
							% NB_OF_THREADS_TO_RECORD_IN_HBASE].submit(
								new DbTask(
									hit,
									partition,
									partitionRecords.get(
										partitionRecords.size() - 1).offset()));
					futuresList.add(
						f);
				}
				futuresList.forEach(
					(f) -> {
						try {
							consumerForTopicWithStringKey.commitSync(
								f.get());
						} catch (CommitFailedException | InterruptedException | ExecutionException e) {
							LOGGER.warn(
								"Error whil commiting ",
								e);
						}
					});
				LOGGER.info(
					"Starting process {} records...",
					records.count());

			} catch (Exception e) {
				LOGGER.error(
					"error in processInputFile ",
					e);
			}
		}
	}

	protected void processIncomingRecord(ConsumerRecord<String, String> rec) {
		Path path = Paths.get(
			rec.value());
		try {

			LOGGER.info(
				"[EVENT][{}] Processing record [ topic: {}, offset: {}, timestamp: {}, path: ]",
				rec.key(),
				rec.topic(),
				rec.offset(),
				rec.timestamp(),
				path.toAbsolutePath());
			Collection<IFD> metaData = beanFileMetadataExtractor.readIFDs(
				path);
			final int nbOfTiffFields = metaData.stream().mapToInt(
				(e) -> e.getTotalNumberOfTiffFields()).sum();
			final IfdContext context = new IfdContext(nbOfTiffFields);
			metaData.forEach(
				(ifd) -> send(
					rec.key(),
					ifd,
					context));
			LOGGER.info(
				"[EVENT][{}] End of Processing record : nb of images {}, nb of exifs {} / total nb of exifs {} ",
				rec.key(),
				context.getCurrentNbOfImages(),
				context.getCurrentTiffId(),
				context.getNbOfTiffFields());
		} catch (Exception e) {
			LOGGER.error(
				"[EVENT][{}] error when processing incoming record " + path.toAbsolutePath(),
				rec.key(),
				e);

		}
	}

	static private class IfdContext {
		protected final int nbOfTiffFields;
		protected int currentNbOfImages;
		protected int currentTiffId;

		IfdContext(
				int nbOfTiffFields) {
			super();
			this.nbOfTiffFields = nbOfTiffFields;
		}

		public void incNbOfImages() {
			currentNbOfImages++;
		}

		public int getNbOfTiffFields() {
			return nbOfTiffFields;
		}

		public int getCurrentNbOfImages() {
			return currentNbOfImages;
		}

		public int getCurrentTiffId() {
			return currentTiffId;
		}

		public void incNbOfTiff() {
			currentTiffId++;
		}

	}

	private void send(String key, IFD ifd, IfdContext context) {
		if (ifd.imageIsPresent()) {
			context.incNbOfImages();
			final String key2 = key + "-IMG-" + context.getCurrentNbOfImages();
			LOGGER.info(
				"[EVENT][{}] publishing a found jpeg image ",
				key2);
			producerForPublishingOnJpegImageTopic.send(
				new ProducerRecord<String, byte[]>(topicJpegFiles, key2, ifd.getJpegImage()));
		}
		ifd.getFields().forEach(
			(f) -> {
				context.incNbOfTiff();
				String tiffKey = key + "-EXIF-" + context.getCurrentTiffId();
				LOGGER.debug(
					"[EVENT][{}] publishing an exif {} ",
					tiffKey,
					f.getData() != null ? f.getData() : " <null> ");
				ExchangedTiffData etd = new ExchangedTiffData(
					key,
					f.getTag().getValue(),
					f.getLength(),
					f.getFieldType(),
					f.getData(),
					f.getTag().toString(),
					tiffKey,
					context.getCurrentTiffId(),
					context.getNbOfTiffFields());
				producerForPublishingOnExifTopic.send(
					new ProducerRecord<String, ExchangedTiffData>(topicExif, key, etd));
			});
		ifd.getAllChildren().forEach(
			(children) -> send(
				key,
				children,
				context));
	}

}
