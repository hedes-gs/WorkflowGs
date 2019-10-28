package com.gs.photo.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("file:${user.home}/config/application.properties")
@ComponentScan(basePackages = "com.gs")
public class WorkflowScan {
	public static void main(String[] args) {
		SpringApplication.run(WorkflowScan.class,
				args);

	}
}
