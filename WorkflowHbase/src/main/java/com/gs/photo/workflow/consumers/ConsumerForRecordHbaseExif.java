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

import com.workflow.model.HbaseData;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseExif extends AbstractConsumerForRecordHbase<HbaseData>
		implements IConsumerForRecordHbaseExif {

	private static Logger                 LOGGER = LogManager.getLogger(ConsumerForRecordHbaseExif.class);

	@Value("${topic.topicExifImageDataToPersist}")
	protected String                      topicExifImageDataToPersist;

	@Autowired
	protected Consumer<String, HbaseData> consumerForRecordingExifDataFromTopic;

	@Override
	public void recordIncomingMessageInHbase() {
		try {
			ConsumerForRecordHbaseExif.LOGGER.info("Start ConsumerForRecordHbaseExif.recordIncomingMessageInHbase");
			this.consumerForRecordingExifDataFromTopic.subscribe(Arrays.asList(this.topicExifImageDataToPersist));
			this.processMessagesFromTopic(this.consumerForRecordingExifDataFromTopic);
		} catch (WakeupException e) {
			ConsumerForRecordHbaseExif.LOGGER.warn("Error ",
					e);
		} finally {
			this.consumerForRecordingExifDataFromTopic.close();
		}
	}

}
