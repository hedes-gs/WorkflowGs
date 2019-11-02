package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.IProcessInputForHashKeyCompute;

@Service
public class BeanProcessInputForHashKeyCompute implements IProcessInputForHashKeyCompute {

	protected final Logger             LOGGER = LoggerFactory.getLogger(IProcessInputForHashKeyCompute.class);
	@Value("${topic.topicScannedFiles}")
	protected String                   topicScanOutput;

	@Value("${topic.topicFileHashKey}")
	protected String                   topicHashKeyOutput;

	@Autowired
	@Qualifier("consumerForTopicWithStringKey")
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	protected Producer<String, String> producerForPublishingOnStringTopic;

	@Autowired
	protected IIgniteDAO               igniteDAO;

	@Autowired
	protected IBeanTaskExecutor        beanTaskExecutor;

	@Autowired
	protected IBeanImageFileHelper     beanImageFileHelper;

	@Override
	public void init() {
		this.beanTaskExecutor.execute(() -> this.processIncomingFile());
	}

	protected void processIncomingFile() {
		this.LOGGER.debug("Starting processing key ");
		this.consumerForTopicWithStringKey.subscribe(Arrays.asList(this.topicScanOutput));
		this.LOGGER.debug("Subscribing done on topic {}",
			this.topicScanOutput);
		Map<String, String> metrics = new HashMap<String, String>();

		while (true) {
			try {
				metrics.clear();
				ConsumerRecords<String, String> records = this.consumerForTopicWithStringKey
					.poll(Duration.ofMillis(500));
				records.forEach((r) -> {
					try {
						byte[] rawFile = this.beanImageFileHelper.readFirstBytesOfFile(r.key(),
							r.value());
						this.LOGGER.info("[EVENT][{}] getting bytes to compute hash key, length is ",
							r.key(),
							rawFile.length);
						String key = this.beanImageFileHelper.computeHashKey(rawFile);
						this.igniteDAO.save(key,
							rawFile);
						this.LOGGER.info("[EVENT][{}] saved in ignite {} ",
							key,
							r.value() + "@" + r.key());
						this.producerForPublishingOnStringTopic
							.send(new ProducerRecord<String, String>(this.topicHashKeyOutput, key, r.value()));
						metrics.put(key,
							r.key());
					} catch (IOException e) {
						this.LOGGER.error("[EVENT][{}] Error when processing key",
							r.key(),
							e);
					}
				});
				this.producerForPublishingOnStringTopic.flush();
				this.consumerForTopicWithStringKey.commitSync();
				metrics.entrySet().forEach((e) -> {
					this.LOGGER.info("[EVENT][{}] Compute hashkey for path {}",
						e.getKey(),
						e.getValue());
				});

			} catch (InterruptException e) {
				this.LOGGER.error("[EVENT][{}] Error when processing ",
					e);
				break;
			} catch (Exception e) {
				this.LOGGER.error("[EVENT][{}] Error when processing ",
					e);

			}
		}
	}

}
