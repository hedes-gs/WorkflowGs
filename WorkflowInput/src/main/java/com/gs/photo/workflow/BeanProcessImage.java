package com.gs.photo.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class BeanProcessImage implements IBeanProcessImage {
	protected static final Logger LOGGER = Logger.getLogger(BeanProcessImage.class);

	@Value("${topic.inputImageNameTopic}")
	protected String imageNameTopic;

	@Value("${topic.pathNameTopic}")
	protected String pathNameTopic;

	@Value("${topic.inputExifTopic}")
	protected String exifTopic;

	@Value("${topic.recordThumbTopic}")
	protected String thumbToProcessTopic;

	@Autowired
	protected IBeanImageFileHelper beanImageFileHelper;

	@Autowired
	protected Producer<String, String> producerForPublishingOnImageTopic;

	@Override
	public void process(String file) {
		Path filePath = new File(file).toPath();

		LOGGER.info("Starting to poll " + file);

		try {
			beanImageFileHelper.waitForCopyComplete(filePath);
			final String key = beanImageFileHelper.computeHashKey(filePath);
			final String fullPathName = beanImageFileHelper.getFullPathName(filePath);

			this.producerForPublishingOnImageTopic
					.send(new ProducerRecord<>(imageNameTopic, key, filePath.getFileName().toString()));
			this.producerForPublishingOnImageTopic.send(new ProducerRecord<>(pathNameTopic, key, fullPathName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
