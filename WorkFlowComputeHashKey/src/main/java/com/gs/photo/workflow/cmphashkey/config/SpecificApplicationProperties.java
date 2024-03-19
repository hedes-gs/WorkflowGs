package com.gs.photo.workflow.cmphashkey.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "application")
public class SpecificApplicationProperties implements IApplicationProperties {

    @Override
    public int batchSizeForParallelProcessingIncomingRecords() { // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int kafkaPollTimeInMillisecondes() { // TODO Auto-generated method stub
        return 0;
    }

}
