package com.gs.photo.workflow.impl;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanScheduleRecordMessageInHbase;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.consumers.ConsumerForRecordHbaseImage;

@Component
public class BeanScheduleRecordMessageInHbase implements IBeanScheduleRecordMessageInHbase {

	protected static final Logger LOGGER = Logger.getLogger(
		BeanScheduleRecordMessageInHbase.class);

	@Autowired
	protected ConsumerForRecordHbaseImage consumerForRecordHbaseImage;

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@PostConstruct
	public void init() {
		beanTaskExecutor.execute(
			() -> {
				consumerForRecordHbaseImage.recordIncomingMessageInHbase();
			});
	}

}
