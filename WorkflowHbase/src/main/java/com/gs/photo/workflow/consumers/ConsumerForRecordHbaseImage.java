package com.gs.photo.workflow.consumers;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.dao.GenericDAO;
import com.workflow.model.HbaseImageThumbnail;

@Component
public class ConsumerForRecordHbaseImage implements IConsumerForRecordHbaseImage {

	private static Logger LOGGER = LogManager.getLogger(
		ConsumerForRecordHbaseImage.class);

	@Value("${topic.recordThumbTopic}")
	protected String topicToRecordThumb;

	@Autowired
	protected Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic;

	@Autowired
	protected GenericDAO genericDAO;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			LOGGER.info(
				"Start ConsumerForRecordHbaseImage.recordIncomingMessageInHbase");
			consumerForRecordingImageFromTopic.subscribe(
				Arrays.asList(
					topicToRecordThumb));
			while (true) {
				ConsumerRecords<String, HbaseImageThumbnail> records = consumerForRecordingImageFromTopic.poll(
					Long.MAX_VALUE);
				LOGGER.info(
					" found {} records ",
					records.count());
				for (ConsumerRecord<String, HbaseImageThumbnail> record : records) {
					genericDAO.put(
						record.value(),
						HbaseImageThumbnail.class);
				}
				try {
					consumerForRecordingImageFromTopic.commitSync();
				} catch (CommitFailedException e) {
					LOGGER.warn(
						"Error whil commiting ",
						e);
				}
			}
		} catch (WakeupException e) {
			LOGGER.warn(
				"Error ",
				e);
		} finally {
			consumerForRecordingImageFromTopic.close();
		}
	}

}
