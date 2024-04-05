package com.gs.photo.workflow.pubthumbimages;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WorkflowPhoto {

	public static void main(String[] args) {
		SpringApplication.run(
			WorkflowPhoto.class,
			args);
	}
}
