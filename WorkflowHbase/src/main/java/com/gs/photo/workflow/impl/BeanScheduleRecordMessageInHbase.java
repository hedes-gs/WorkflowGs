package com.gs.photo.workflow.impl;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanScheduleRecordMessageInHbase;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.consumers.IConsumerForRecordHbaseExif;
import com.gs.photo.workflow.consumers.IConsumerForRecordHbaseImage;
import com.gs.photo.workflow.consumers.IConsumerForRecordHbaseImageExif;

@Component
public class BeanScheduleRecordMessageInHbase implements IBeanScheduleRecordMessageInHbase {

	protected static final Logger LOGGER = Logger.getLogger(
		BeanScheduleRecordMessageInHbase.class);

	@Autowired
	protected IConsumerForRecordHbaseImage consumerForRecordHbaseImage;

	@Autowired
	protected IConsumerForRecordHbaseExif consumerForRecordHbaseExif;

	@Autowired
	protected IConsumerForRecordHbaseImageExif consumerForRecordHbaseImageExif;

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@PostConstruct
	public void init() {
		beanTaskExecutor.executeRunnables(
			Arrays.asList(
				() -> {
					consumerForRecordHbaseImage.recordIncomingMessageInHbase();
				},
				() -> {
					consumerForRecordHbaseImageExif.recordIncomingMessageInHbase();
				},
				() -> {
					consumerForRecordHbaseExif.recordIncomingMessageInHbase();
				}));
	}

}
