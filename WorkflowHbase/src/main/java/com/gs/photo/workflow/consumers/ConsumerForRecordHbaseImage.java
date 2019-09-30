package com.gs.photo.workflow.consumers;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.workflow.model.HbaseImageThumbnail;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImage extends AbstractConsumerForRecordHbase<HbaseImageThumbnail>
		implements IConsumerForRecordHbaseImage {

	private static Logger LOGGER = LogManager.getLogger(
		ConsumerForRecordHbaseImage.class);

	@Value("${topic.exifData}")
	protected String topic;

	@Autowired
	protected Consumer<String, HbaseImageThumbnail> consumerToRecordHbaseImage;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			LOGGER.info(
				"Start ConsumerForRecordHbaseImage.recordIncomingMessageInHbase");
			consumerToRecordHbaseImage.subscribe(
				Arrays.asList(
					topic));
			processMessagesFromTopic(
				consumerToRecordHbaseImage,
				HbaseImageThumbnail.class);
		} catch (WakeupException e) {
			LOGGER.warn(
				"Error ",
				e);
		} finally {
			consumerToRecordHbaseImage.close();
		}
	}

}
