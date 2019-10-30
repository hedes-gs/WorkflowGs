package com.gs.photo.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@PropertySource("file:${user.home}/config/application.properties")
@EnableAsync
public class WorkflowExtractImageInfo {

	private static ApplicationContext applicationContext;

	public static void main(String[] args) {
		WorkflowExtractImageInfo.applicationContext = SpringApplication.run(WorkflowExtractImageInfo.class,
				args);
		IProcessIncomingFiles iprocessIncomingFiles = WorkflowExtractImageInfo.applicationContext
				.getBean(IProcessIncomingFiles.class);
		iprocessIncomingFiles.init();
	}
}
