package com.gs.photo.workflow.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

	private static final int FORK_JOIN_PARALLELISM = 10;

	protected static Logger LOGGER = LoggerFactory.getLogger(
		IProcessIncomingFiles.class);

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
		beanTaskExecutor.execute(
			() -> processInputFile());
	}

	protected void processInputFile() {
		consumerForTopicWithStringKey.subscribe(
			Collections.singleton(
				topicCopyFile));
		ForkJoinPool pool = new ForkJoinPool(FORK_JOIN_PARALLELISM);
		LOGGER.info(
			"Starting process input file...");
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumerForTopicWithStringKey.poll(
					500);
				LOGGER.info(
					"Starting process {} records...",
					records.count());

				pool.submit(
					() -> StreamSupport.stream(
						records.spliterator(),
						true).parallel().forEach(
							(rec) -> processIncomingRecord(
								rec)));
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
