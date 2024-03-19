package com.gs.photo.workflow.copyfiles;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkFlowCopyFile {

    public static void main(String[] args) { SpringApplication.run(WorkFlowCopyFile.class, args); }
}
