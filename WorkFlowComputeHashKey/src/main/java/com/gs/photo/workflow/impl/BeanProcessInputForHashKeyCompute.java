package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IBeanImageFileHelper;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IProcessInputForHashKeyCompute;

@Service
public class BeanProcessInputForHashKeyCompute implements IProcessInputForHashKeyCompute {

	protected final Logger LOGGER = LoggerFactory.getLogger(
		IProcessInputForHashKeyCompute.class);
	@Value("${topic.scan-output}")
	protected String topicScanOutput;

	@Value("${topic.hashkey-output}")
	protected String topicHashKeyOutput;

	@Autowired
	@Qualifier("consumerForTopicWithStringKey")
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	protected Producer<String, String> producerForPublishingOnStringTopic;

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@Autowired
	protected IBeanImageFileHelper beanImageFileHelper;

	@PostConstruct
	public void init() {
		beanTaskExecutor.execute(
			() -> processIncomingFile());
	}

	protected void processIncomingFile() {
		LOGGER.debug(
			"Starting processing key ");
		consumerForTopicWithStringKey.subscribe(
			Arrays.asList(
				topicScanOutput));
		LOGGER.debug(
			"Subscribing done on topic {}",
			topicScanOutput);
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumerForTopicWithStringKey.poll(
					100);
				records.forEach(
					(r) -> {
						try {
							String key = beanImageFileHelper.computeHashKey(
								Paths.get(
									r.key()));
							LOGGER.info(
								"[EVENT][{}] Compute hashkey {}",
								r.key(),
								key);
							producerForPublishingOnStringTopic.send(
								new ProducerRecord<String, String>(topicHashKeyOutput, key, r.value()));
						} catch (IOException e) {
							LOGGER.error(
								"[EVENT][{}] Error when processing key",
								r.key(),
								e);
						}
					});
				producerForPublishingOnStringTopic.flush();
				consumerForTopicWithStringKey.commitSync();

			} catch (InterruptException e) {
				LOGGER.error(
					"[EVENT][{}] Error when processing ",
					e);
				break;
			} catch (Exception e) {
				LOGGER.error(
					"[EVENT][{}] Error when processing ",
					e);

			}
		}
	}

}
