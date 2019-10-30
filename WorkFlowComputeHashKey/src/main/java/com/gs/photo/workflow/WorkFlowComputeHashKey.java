package com.gs.photo.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@PropertySource("file:${user.home}/config/application.properties")
@EnableAsync
public class WorkFlowComputeHashKey {
	public static void main(String[] args) {
		ApplicationContext ac = SpringApplication.run(WorkFlowComputeHashKey.class,
				args);
		ac.getBean(IProcessInputForHashKeyCompute.class).init();
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
