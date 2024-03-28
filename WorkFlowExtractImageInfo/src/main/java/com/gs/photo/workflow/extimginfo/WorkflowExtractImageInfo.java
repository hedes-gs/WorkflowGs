package com.gs.photo.workflow.extimginfo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkflowExtractImageInfo {

    public static class GreetingInterceptor {
        public Object greet(Object argument) { return "Herllo from " + argument; }
    }

    public static void main(String[] args) {

        SpringApplication.run(WorkflowExtractImageInfo.class, args);
    }
}
