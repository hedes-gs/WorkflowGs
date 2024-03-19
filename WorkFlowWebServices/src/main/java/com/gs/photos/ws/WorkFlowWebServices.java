package com.gs.photos.ws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.hateoas.config.EnableHypermediaSupport;
import org.springframework.hateoas.config.EnableHypermediaSupport.HypermediaType;

@SpringBootApplication(proxyBeanMethods = false)
@EnableHypermediaSupport(type = HypermediaType.HAL)
public class WorkFlowWebServices {

    public static void main(String[] args) {
        final SpringApplication application = new SpringApplication(WorkFlowWebServices.class);
        application.setWebApplicationType(WebApplicationType.REACTIVE);
        SpringApplication.run(WorkFlowWebServices.class, args);

    }
}
