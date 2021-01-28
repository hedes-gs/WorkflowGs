package com.gs.photo.workflow.recinhbase.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.recinhbase.IBeanScheduleRecordMessageInHbase;
import com.gs.photo.workflow.recinhbase.consumers.IConsumerForRecordHbaseExif;
import com.gs.photo.workflow.recinhbase.consumers.IConsumerForRecordHbaseImage;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class BeanScheduleRecordMessageInHbase implements IBeanScheduleRecordMessageInHbase {

    protected static final Logger          LOGGER = LoggerFactory.getLogger(BeanScheduleRecordMessageInHbase.class);

    @Autowired
    protected IConsumerForRecordHbaseImage consumerForRecordHbaseImage;

    @Autowired
    protected IConsumerForRecordHbaseExif  consumerForRecordHbaseExif;

    @Autowired
    protected IBeanTaskExecutor            beanTaskExecutor;

    @PostConstruct
    public void init() {

    }

}
