package com.gs.photo.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkflowAgentApplication {
    private static Logger LOGGER = LoggerFactory.getLogger(WorkflowAgentApplication.class);

    public static void main(String[] args) {

        WorkflowAgentApplication.LOGGER.info(" Start application ");
        try {
            ApplicationContext ctx = SpringApplication.run(WorkflowAgentApplication.class, args);
            WorkflowAgentApplication.LOGGER.info(" End of spring init application ");
        } catch (Exception e) {
            WorkflowAgentApplication.LOGGER.error("Unable to start APPLIUCATION", e);
        }
    }

}
