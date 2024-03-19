package com.gs.photo.workflow.cmphashkey.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.IIgniteProperties;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ignite.caches")
public class IgniteProperties implements IIgniteProperties {

    protected String              defaultCache;
    protected Map<String, String> cachesPerclasses;

    @Override
    public String getDefaultCache() { return this.defaultCache; }

    public void setDefaultCache(String defaultCache) { this.defaultCache = defaultCache; }

    @Override
    public Map<String, String> getCachesPerclasses() { return this.cachesPerclasses; }

    public void setCachesPerclasses(Map<String, String> cachesPerclasses) { this.cachesPerclasses = cachesPerclasses; }

    @Override
    public String toString() {
        return "IgniteProperties [defaultCache=" + this.defaultCache + ", cachesPerclasses=" + this.cachesPerclasses
            + "]";
    }

}
