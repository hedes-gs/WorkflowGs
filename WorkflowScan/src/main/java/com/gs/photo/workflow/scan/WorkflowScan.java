package com.gs.photo.workflow.scan;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan
public class WorkflowScan {
    private static Logger LOGGER = LoggerFactory.getLogger(WorkflowScan.class);

    public static void main(String[] args) {

        WorkflowScan.LOGGER.info(" Start application ");
        try {
            ApplicationContext ctx = SpringApplication.run(WorkflowScan.class, args);
            WorkflowScan.LOGGER.info(" End of spring init application ");
            try {
                final CountDownLatch shutdownCoseLatch = ctx.getBean(CountDownLatch.class);
                WorkflowScan.LOGGER.info(
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
                WorkflowScan.LOGGER.warn(" unexpected error", e);
            }
        } catch (Exception e) {
            WorkflowScan.LOGGER.error("Unable to start APPLIUCATION", e);
        }
    }

}
