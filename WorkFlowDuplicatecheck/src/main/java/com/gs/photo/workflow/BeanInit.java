package com.gs.photo.workflow;

import javax.annotation.PostConstruct;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BeanInit {

	@Autowired
	protected IDuplicateCheck beanDuplicateCheck;

	@PostConstruct
	public void init() {
		KafkaStreams ks = beanDuplicateCheck.buildKafkaStreamsTopology();
		ks.start();
	}
}
