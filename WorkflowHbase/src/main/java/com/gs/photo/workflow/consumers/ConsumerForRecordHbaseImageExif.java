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

import com.workflow.model.HbaseExifDataOfImages;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImageExif extends AbstractConsumerForRecordHbase<HbaseExifDataOfImages>
		implements IConsumerForRecordHbaseImageExif {

	private static Logger LOGGER = LogManager.getLogger(
		ConsumerForRecordHbaseImageExif.class);

	@Value("${topic.exifData}")
	protected String topic;

	@Autowired
	protected Consumer<String, HbaseExifDataOfImages> consumerToRecordExifDataOfImages;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			LOGGER.info(
				"Start ConsumerForRecordHbaseExif.recordIncomingMessageInHbase");
			consumerToRecordExifDataOfImages.subscribe(
				Arrays.asList(
					topic));
			processMessagesFromTopic(
				consumerToRecordExifDataOfImages,
				HbaseExifDataOfImages.class);
		} catch (WakeupException e) {
			LOGGER.warn(
				"Error ",
				e);
		} finally {
			consumerToRecordExifDataOfImages.close();
		}
	}

}
