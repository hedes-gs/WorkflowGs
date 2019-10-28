package com.gs.photo.workflow;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
public class ApplicationConfig extends AbstractApplicationConfig {

}
