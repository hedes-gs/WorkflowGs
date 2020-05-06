package com.gs.photo.workflow;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@PropertySource("file:${user.home}/config/application.properties")
@EnableAsync
public class WorkflowHbaseApplication {
    private static Logger LOGGER = LoggerFactory.getLogger(WorkflowHbaseApplication.class);

    public static void main(String[] args) {

        WorkflowHbaseApplication.LOGGER.info(" Start application ");
        try {
            ApplicationContext ctx = SpringApplication.run(WorkflowHbaseApplication.class, args);
            WorkflowHbaseApplication.LOGGER.info(" End of spring init application ");
            try {
                final CountDownLatch shutdownCoseLatch = ctx.getBean(CountDownLatch.class);
                WorkflowHbaseApplication.LOGGER.info(
                    "Wait for a Kill application on ",
                    ProcessHandle.current()
                        .pid());
                Runtime.getRuntime()
                    .addShutdownHook(new Thread() {
                        @Override
                        public void run() { shutdownCoseLatch.countDown(); }
                    });
                try {
                    shutdownCoseLatch.await();
                } catch (InterruptedException e) {
                }
            } catch (Exception e) {
                WorkflowHbaseApplication.LOGGER.warn(" unexpected error", e);
            }
        } catch (Exception e) {
            WorkflowHbaseApplication.LOGGER.error("Unable to start APPLIUCATION", e);
        }
    }

}
