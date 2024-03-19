package com.gs.photo.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkFlowMonitor {

    public static void main(String[] args) {
        SpringApplication.run(WorkFlowMonitor.class, args);
        try {
            synchronized (SpringApplication.class) {
                SpringApplication.class.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
