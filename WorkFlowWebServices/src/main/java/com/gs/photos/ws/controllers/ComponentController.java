package com.gs.photos.ws.controllers;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.workflow.model.events.ImportEvent;

@RestController
@RequestMapping("/api/gs")
public class ComponentController {

    protected static Logger                    LOGGER = LoggerFactory.getLogger(ComponentController.class);

    @Autowired
    private KafkaTemplate<String, ImportEvent> kafkaTemplate;

    @Value("${topic.topicImportEvent}")
    protected String                           topicImportEvent;

    @PostMapping(path = "/import/start")
    public boolean startImport(@RequestBody ImportEvent importEvent) throws IOException {
        ComponentController.LOGGER.info("Request to start import with {}", importEvent);
        importEvent.setForTest(false);
        importEvent.setNbMaxOfImages(500);
        this.kafkaTemplate.send(this.topicImportEvent, importEvent);
        return true;
    }
}
