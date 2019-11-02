package com.gs.photo.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@PropertySource("file:${user.home}/config/application.properties")
@EnableAsync
public class WorkFlowProcessAndPublishExifData {

	public static void main(String[] args) {
		SpringApplication.run(WorkFlowProcessAndPublishExifData.class,
			args);
		try {
			synchronized (SpringApplication.class) {
				SpringApplication.class.wait();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
