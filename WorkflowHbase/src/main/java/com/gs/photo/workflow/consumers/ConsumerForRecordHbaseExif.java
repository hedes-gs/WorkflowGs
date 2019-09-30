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

import com.workflow.model.HbaseExifData;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseExif extends AbstractConsumerForRecordHbase<HbaseExifData>
		implements IConsumerForRecordHbaseExif {

	private static Logger LOGGER = LogManager.getLogger(
		ConsumerForRecordHbaseExif.class);

	@Value("${topic.exifData}")
	protected String topic;

	@Autowired
	protected Consumer<String, HbaseExifData> consumerForRecordingExifDataFromTopic;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			LOGGER.info(
				"Start ConsumerForRecordHbaseExif.recordIncomingMessageInHbase");
			consumerForRecordingExifDataFromTopic.subscribe(
				Arrays.asList(
					topic));
			processMessagesFromTopic(
				consumerForRecordingExifDataFromTopic,
				HbaseExifData.class);
		} catch (WakeupException e) {
			LOGGER.warn(
				"Error ",
				e);
		} finally {
			consumerForRecordingExifDataFromTopic.close();
		}
	}

}
