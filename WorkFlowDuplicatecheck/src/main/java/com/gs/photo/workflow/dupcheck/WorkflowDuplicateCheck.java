package com.gs.photo.workflow.dupcheck;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkflowDuplicateCheck {

    public static void main(String[] args) { SpringApplication.run(WorkflowDuplicateCheck.class, args); }
}
