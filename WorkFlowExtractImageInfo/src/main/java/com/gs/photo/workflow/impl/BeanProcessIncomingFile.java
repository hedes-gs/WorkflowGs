package com.gs.photo.workflow.impl;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IProcessIncomingFiles;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.tiff.TiffField;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;

@Component
public class BeanProcessIncomingFile implements IProcessIncomingFiles {

	static private class IfdContext {
		protected final int   nbOfTiffFields;
		protected int         currentNbOfImages;
		protected int         currentTiffId;
		protected List<Short> currentIFDStack;

		IfdContext(
			int nbOfTiffFields) {
			super();
			this.nbOfTiffFields = nbOfTiffFields;
			this.currentIFDStack = new ArrayList<>(5);
		}

		public void incNbOfImages() {
			this.currentNbOfImages++;
		}

		public int getNbOfTiffFields() {
			return this.nbOfTiffFields;
		}

		public int getCurrentNbOfImages() {
			return this.currentNbOfImages;
		}

		public int getCurrentTiffId() {
			return this.currentTiffId;
		}

		public void incNbOfTiff() {
			this.currentTiffId++;
		}

		public Short pop() {
			if (this.currentIFDStack.size() > 0) {
				return this.currentIFDStack.remove(this.currentIFDStack.size() - 1);
			}
			return 0;
		}

		public void push(Short tag) {
			this.currentIFDStack.add(tag);
		}

		public short[] resolvePath() {
			short[] retValue = new short[this.currentIFDStack.size()];
			for (int k = 0; k < this.currentIFDStack.size(); k++) {
				retValue[k] = this.currentIFDStack.get(k);
			}
			return retValue;
		}

	}

	public static final int     NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

	protected static Logger     LOGGER                           = LoggerFactory.getLogger(IProcessIncomingFiles.class);
	protected ExecutorService[] services;

	protected static class DbTaskResult {
		private final TopicPartition                partition;
		private final OffsetAndMetadata             offsetAndMetadata;
		private final Table<String, String, Object> objectsToSend;
		private final Map<String, Integer>          metrics;

		public TopicPartition getPartition() {
			return this.partition;
		}

		public OffsetAndMetadata getOffsetAndMetadata() {
			return this.offsetAndMetadata;
		}

		public Table<String, String, Object> getObjectsToSend() {
			return this.objectsToSend;
		}

		public Map<String, Integer> getMetrics() {
			return this.metrics;
		}

		public DbTaskResult(
			TopicPartition partition,
			OffsetAndMetadata offsetAndMetadata,
			Table<String, String, Object> objectsToSend,
			Map<String, Integer> metrics) {
			super();
			this.partition = partition;
			this.offsetAndMetadata = offsetAndMetadata;
			this.objectsToSend = objectsToSend;
			this.metrics = metrics;
		}

	}

	protected final class DbTask implements Callable<DbTaskResult> {
		protected List<ConsumerRecord<String, String>> records;
		protected TopicPartition                       partition;
		protected long                                 lastOffset;

		public DbTask(
			List<ConsumerRecord<String, String>> records,
			TopicPartition partition,
			long offsetToCommit) {
			super();
			this.records = records;
			this.partition = partition;
			this.lastOffset = offsetToCommit + 1;
		}

		@Override
		public DbTaskResult call() throws Exception {
			Table<String, String, Object> objectsToSend = HashBasedTable.create();
			Map<String, Integer> metrics = new HashMap<>();
			for (ConsumerRecord<String, String> record : this.records) {
				BeanProcessIncomingFile.this.processIncomingRecord(record,
					objectsToSend,
					metrics);
			}
			DbTaskResult result = new DbTaskResult(this.partition,
				new OffsetAndMetadata(this.lastOffset + 1),
				objectsToSend,
				metrics);
			return result;
		}
	}

	@Autowired
	protected IBeanTaskExecutor        beanTaskExecutor;

	@Value("${topic.topicDupFilteredFile}")
	protected String                   topicDupFilteredFile;

	@Value("${topic.topicExif}")
	protected String                   topicExif;

	@Value("${topic.topicThumb}")
	protected String                   topicThumb;

	@Value("${group.id}")
	private String                     groupId;

	@Autowired
	protected IFileMetadataExtractor   beanFileMetadataExtractor;

	@Autowired
	@Qualifier("consumerForTopicWithStringKey")
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	@Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
	protected Producer<String, Object> producerForTransactionPublishingOnExifOrImageTopic;

	@Override
	public void init() {
		this.services = new ExecutorService[BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE];
		for (int k = 0; k < BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE; k++) {
			this.services[k] = Executors.newFixedThreadPool(1);
		}
		this.beanTaskExecutor.execute(() -> this.processInputFile());
	}

	protected void processInputFile() {
		this.consumerForTopicWithStringKey.subscribe(Collections.singleton(this.topicDupFilteredFile));
		BeanProcessIncomingFile.LOGGER.info("Starting process input file...");
		Collection<Future<DbTaskResult>> futuresList = new ArrayList<>();
		this.producerForTransactionPublishingOnExifOrImageTopic.initTransactions();
		Map<String, Long> nbOfSentElements = new HashMap<String, Long>();
		Map<String, Integer> metrics = new HashMap<>();

		while (true) {
			try {
				nbOfSentElements.clear();
				futuresList.clear();
				metrics.clear();
				ConsumerRecords<String, String> records = this.consumerForTopicWithStringKey
					.poll(Duration.ofMillis(500));
				if (!records.isEmpty()) {
					BeanProcessIncomingFile.LOGGER.info("Processing {} records",
						records.count());
					this.producerForTransactionPublishingOnExifOrImageTopic.beginTransaction();
					for (TopicPartition partition : records.partitions()) {
						List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
						List<ConsumerRecord<String, String>> foundRecords = new ArrayList<>();
						for (ConsumerRecord<String, String> record : partitionRecords) {
							foundRecords.add(record);
						}
						Future<DbTaskResult> f = this.services[partition.partition()
							% BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE]
								.submit(new DbTask(foundRecords,
									partition,
									partitionRecords.get(partitionRecords.size() - 1).offset()));
						futuresList.add(f);
					}
					Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
					futuresList.forEach((f) -> {
						try {
							DbTaskResult taskResult = f.get();
							offsets.put(taskResult.getPartition(),
								taskResult.getOffsetAndMetadata());
							taskResult.getObjectsToSend().rowKeySet().forEach((topic) -> {
								taskResult.getObjectsToSend().row(topic).entrySet().forEach((e) -> {
									ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic,
										e.getKey(),
										e.getValue());
									this.producerForTransactionPublishingOnExifOrImageTopic.send(producerRecord);
								});

							});
							metrics.putAll(taskResult.getMetrics());
						} catch (
							CommitFailedException |
							InterruptedException |
							ExecutionException e) {
							BeanProcessIncomingFile.LOGGER.warn("Error whil commiting ",
								e);
						}
					});
					this.producerForTransactionPublishingOnExifOrImageTopic.sendOffsetsToTransaction(offsets,
						this.groupId);
					this.producerForTransactionPublishingOnExifOrImageTopic.commitTransaction();
					metrics.entrySet().forEach((e) -> {
						BeanProcessIncomingFile.LOGGER.info(
							"[EVENT][{}] processed file of hash key {}, sent {} Exif or image values ",
							e.getKey(),
							e.getValue());
					});
				}
			} catch (Exception e) {
				BeanProcessIncomingFile.LOGGER.error("error in processInputFile ",
					e);
			}
		}
	}

	protected void processIncomingRecord(ConsumerRecord<String, String> rec,
		Table<String, String, Object> objectsToSend, Map<String, Integer> metrics) {
		try {

			BeanProcessIncomingFile.LOGGER.info(
				"[EVENT][{}] Processing record [ topic: {}, offset: {}, timestamp: {}, path: ]",
				rec.key(),
				rec.topic(),
				rec.offset(),
				rec.timestamp(),
				rec.key());
			Collection<IFD> metaData = this.beanFileMetadataExtractor.readIFDs(rec.key());
			final int nbOfTiffFields = metaData.stream().mapToInt((e) -> e.getTotalNumberOfTiffFields()).sum();
			final IfdContext context = new IfdContext(nbOfTiffFields);
			metaData.forEach((ifd) -> {
				context.push(ifd.getTag().getValue());
				this.buildObjectsToSend(rec.key(),
					ifd,
					context,
					objectsToSend,
					metrics);
			});

			BeanProcessIncomingFile.LOGGER.info(
				"[EVENT][{}] End of Processing record : nb of images {}, nb of exifs {} / total nb of exifs {} ",
				rec.key(),
				context.getCurrentNbOfImages(),
				context.getCurrentTiffId(),
				context.getNbOfTiffFields());
		} catch (Exception e) {
			BeanProcessIncomingFile.LOGGER.error("[EVENT][{}] error when processing incoming record ",
				rec.key(),
				e);

		}
	}

	private void buildObjectsToSend(String key, IFD ifd, IfdContext context,
		Table<String, String, Object> objectsToSend, Map<String, Integer> metrics) {
		context.push(ifd.getTag().getValue());
		if (ifd.imageIsPresent()) {
			context.incNbOfImages();
			final String key2 = key + "-IMG-" + context.getCurrentNbOfImages();
			BeanProcessIncomingFile.LOGGER.info("[EVENT][{}] publishing a found jpeg image ",
				key2);
			metrics.merge(key,
				1,
				Integer::sum);
			objectsToSend.put(this.topicThumb,
				key2,
				ifd.getJpegImage());
		}
		ifd.getFields().forEach((f) -> {
			try {
				context.incNbOfTiff();
				ExchangedTiffData etd = this.buildExchangedTiffData(key,
					context,
					f);
				Object retValue = objectsToSend.put(this.topicExif,
					etd.getKey(),
					etd);
				if (retValue != null) {
					BeanProcessIncomingFile.LOGGER.error(
						"Error double values found for [{}, {}] : {} , previous value is : {}",
						this.topicExif,
						etd.getId(),
						etd,
						retValue);
				} else {
					metrics.merge(key,
						1,
						Integer::sum);
				}
			} catch (UnsupportedEncodingException e) {
				BeanProcessIncomingFile.LOGGER.error("Error",
					e);
			}
		});
		ifd.getAllChildren()
			.forEach((children) -> this.buildObjectsToSend(key,
				children,
				context,
				objectsToSend,
				metrics));
		context.pop();
	}

	protected ExchangedTiffData buildExchangedTiffData(String key, IfdContext context, TiffField<?> f)
		throws UnsupportedEncodingException {
		String tiffKey = key + "-EXIF-" + context.getCurrentTiffId();
		BeanProcessIncomingFile.LOGGER.debug("[EVENT][{}] publishing an exif {} ",
			tiffKey,
			f.getData() != null ? f.getData() : " <null> ");
		ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
		Object internalData = f.getData();
		if (internalData instanceof int[]) {
			builder.withDataAsInt((int[]) internalData);
		} else if (internalData instanceof short[]) {
			builder.withDataAsShort((short[]) internalData);
		} else if (internalData instanceof byte[]) {
			builder.withDataAsByte((byte[]) internalData);
		} else if (internalData instanceof String) {
			builder.withDataAsByte(((String) internalData).getBytes("UTF-8"));
		} else {
			throw new IllegalArgumentException();
		}
		short[] path = context.resolvePath();
		builder.withImageId(key)
			.withKey(tiffKey)
			.withTag(f.getTag().getValue())
			.withLength(f.getLength())
			.withFieldType(FieldType.fromShort(f.getFieldType()))
			.withIntId(context.getCurrentTiffId())
			.withTotal(context.getNbOfTiffFields())
			.withId(f.getTag().toString())
			.withPath(path);
		ExchangedTiffData etd = builder.build();
		return etd;
	}

}
