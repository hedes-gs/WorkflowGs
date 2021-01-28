package com.gs.photo.workflow;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {
    private static final String HBASE_MASTER_KERBEROS_PRINCIPAL       = "hbase.master.kerberos.principal";
    private static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
    private static final String HBASE_RPC_PROTECTION                  = "hbase.rpc.protection";
    private static final String HBASE_SECURITY_AUTHENTICATION         = "hbase.security.authentication";
    private static final String HADOOP_SECURITY_AUTHENTICATION        = "hadoop.security.authentication";
    public static final String  CONSUMER_IMAGE                        = "consumer-image";
    public static final String  CONSUMER_EXIF                         = "consumer-exif";

    private static Logger       LOGGER                                = LogManager.getLogger(ApplicationConfig.class);

    @Bean
    protected org.apache.hadoop.conf.Configuration hbaseConfiguration(
        @Value("${zookeeper.hosts}") String zookeeperHosts,
        @Value("${zookeeper.port}") int zookeeperPort
    ) {

        org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zookeeperHosts);
        hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
        hBaseConfig.set(ApplicationConfig.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(ApplicationConfig.HBASE_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(HConstants.CLUSTER_DISTRIBUTED, "true");
        hBaseConfig.set(ApplicationConfig.HBASE_RPC_PROTECTION, "authentication");
        hBaseConfig.set(ApplicationConfig.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");
        hBaseConfig.set(ApplicationConfig.HBASE_MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");

        return hBaseConfig;
    }

    @Bean
    public Connection hbaseConnection(
        @Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
        @Value("${application.gs.principal}") String principal,
        @Value("${application.gs.keytab}") String keytab
    ) {
        ApplicationConfig.LOGGER.info("creating the hbase connection");
        UserGroupInformation.setConfiguration(hbaseConfiguration);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
        PrivilegedAction<Connection> action = () -> {
            try {
                return ConnectionFactory.createConnection(hbaseConfiguration);
            } catch (IOException e) {
                ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            }
            return null;
        };
        try {
            return UserGroupInformation.getCurrentUser()
                .doAs(action);
        } catch (IOException e) {
            ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
    }

    @Bean(name = "hdfsFileSystem")
    public FileSystem hdfsFileSystem(
        @Value("${application.gs.principal}") String principal,
        @Value("${application.gs.keytab}") String keyTab
    ) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            ApplicationConfig.LOGGER.info("Kerberos Login from login {} and keytab {}", principal, keyTab);
        } catch (IOException e1) {
            ApplicationConfig.LOGGER
                .warn("Error when login {},{} : {}", principal, keyTab, ExceptionUtils.getStackTrace(e1));
            throw new RuntimeException(e1);
        }
        PrivilegedAction<FileSystem> action = () -> {
            FileSystem retValue = null;
            try {
                retValue = FileSystem.get(configuration);
            } catch (IOException e) {
                ApplicationConfig.LOGGER.warn(
                    "Error when Getting FileSystem {},{} : {}",
                    principal,
                    keyTab,
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
            return retValue;
        };
        try {
            return UserGroupInformation.getLoginUser()
                .doAs(action);
        } catch (IOException e) {
            ApplicationConfig.LOGGER
                .warn("Error when login {},{} : {}", principal, keyTab, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

}
