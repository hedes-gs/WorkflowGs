package com.gs.photo.workflow;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("file:${user.home}/config/cluster-client.xml")
public class ApplicationConfig extends AbstractApplicationConfig {

}
