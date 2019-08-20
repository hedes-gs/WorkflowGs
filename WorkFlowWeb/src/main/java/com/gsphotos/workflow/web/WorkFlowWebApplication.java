package com.gsphotos.workflow.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication(scanBasePackages = { "com.gsphotos.workflow.web", "com.gsphotos.workflow.dao" })
public class WorkFlowWebApplication {

	public static void main(String[] args) {
		SpringApplication.run(WorkFlowWebApplication.class, args);
	}
}
