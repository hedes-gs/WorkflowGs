package com.gs.photo.workflow.extimginfo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkflowExtractImageInfo {

    private static ApplicationContext applicationContext;

    public static void main(String[] args) {
        WorkflowExtractImageInfo.applicationContext = SpringApplication.run(WorkflowExtractImageInfo.class, args);
        IProcessIncomingFiles iprocessIncomingFiles = WorkflowExtractImageInfo.applicationContext
            .getBean(IProcessIncomingFiles.class);
        iprocessIncomingFiles.init();
    }
}
