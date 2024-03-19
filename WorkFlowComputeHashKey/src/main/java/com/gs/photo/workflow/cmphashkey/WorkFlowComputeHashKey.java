package com.gs.photo.workflow.cmphashkey;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@EnableConfigurationProperties
public class WorkFlowComputeHashKey {

    private static Logger LOGGER = LoggerFactory.getLogger(WorkFlowComputeHashKey.class);

    public static void main(String[] args) {
        WorkFlowComputeHashKey.LOGGER.info(" Start application ");
        try {
            ApplicationContext ctx = SpringApplication.run(WorkFlowComputeHashKey.class, args);
            WorkFlowComputeHashKey.LOGGER.info(" End of spring init application ");
            try {
                final CountDownLatch shutdownCoseLatch = ctx.getBean(CountDownLatch.class);
                WorkFlowComputeHashKey.LOGGER.info(
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
                WorkFlowComputeHashKey.LOGGER.warn(" unexpected error", e);
            }
        } catch (Exception e) {
            WorkFlowComputeHashKey.LOGGER.error("Unable to start APPLIUCATION", e);
        }
    }
}
