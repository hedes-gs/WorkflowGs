package com.gs.photos.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.workflow.model.dtos.ImageDto;
import com.workflow.model.events.ComponentEvent;

@Service
public class KafkaConsumerService {

    protected static Logger         LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    protected SimpMessagingTemplate template;

    public void checkout(ImageDto imgKeyDto) {

    }

    @KafkaListener(topics = "${topic.topicComponentStatus}")
    public void consume(@Payload(required = false) ComponentEvent message) {
        if (message != null) {
            this.template.convertAndSend("/topic/componentStatus", message);
        } else {
            KafkaConsumerService.LOGGER.warn("Kafka : Receive message null !");
        }
    }

}