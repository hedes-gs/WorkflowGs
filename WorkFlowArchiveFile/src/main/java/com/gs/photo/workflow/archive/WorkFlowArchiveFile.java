package com.gs.photo.workflow.archive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkFlowArchiveFile {

    public static void main(String[] args) { SpringApplication.run(WorkFlowArchiveFile.class, args); }
}
