package com.gs.photo.workflow.consumers;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.workflow.model.HbaseExifData;

@Component
public class ConsumerForRecordHbaseExif extends AbstractConsumerForRecordHbase<HbaseExifData>
		implements IConsumerForRecordHbaseExif {

	private static Logger LOGGER = LogManager.getLogger(
		ConsumerForRecordHbaseExif.class);

	@Value("${topic.exifData}")
	protected String topic;

	@Autowired
	protected Consumer<String, HbaseExifData> consumerToRecordExifData;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			LOGGER.info(
				"Start ConsumerForRecordHbaseExif.recordIncomingMessageInHbase");
			consumerToRecordExifData.subscribe(
				Arrays.asList(
					topic));
			processMessagesFromTopic(
				consumerToRecordExifData,
				HbaseExifData.class);
		} catch (WakeupException e) {
			LOGGER.warn(
				"Error ",
				e);
		} finally {
			consumerToRecordExifData.close();
		}
	}

}
