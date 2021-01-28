package com.gs.photo.workflow.dupcheck;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class BeanInit {

	@Autowired
	protected IDuplicateCheck beanDuplicateCheck;

	@Value("${deduplication.cleanup}")
	protected boolean         cleanUp;

	@PostConstruct
	public void init() {
		KafkaStreams ks = this.beanDuplicateCheck.buildKafkaStreamsTopology();
		if (this.cleanUp) {
			ks.cleanUp();
		}
		ks.start();
	}
}
