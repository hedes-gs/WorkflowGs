package com.gsphotos.workflow.web;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.google.common.collect.ImmutableMap;
import com.gsphotos.workflow.web.scopes.ViewScope;

@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfig {

	@Value("${zookeeper.host}")
	protected String zookeeperHost;

	@Value("${zookeeper.port}")
	protected String zookeeperPort;

	@Value("${hbase.host}")
	protected String hbaseHost;

	@Bean
	public org.apache.hadoop.conf.Configuration hBaseConfig() {
		org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
		hBaseConfig.setInt("timeout", 120000);
		hBaseConfig.set("hbase.master", "*" + hbaseHost + ":9000*");
		hBaseConfig.set("hbase.zookeeper.quorum", zookeeperHost);
		hBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
		return hBaseConfig;
	}

	@Bean
	public static CustomScopeConfigurer viewScope() {
		CustomScopeConfigurer configurer = new CustomScopeConfigurer();
		configurer.setScopes(new ImmutableMap.Builder<String, Object>().put("view", new ViewScope()).build());
		return configurer;
	}

}