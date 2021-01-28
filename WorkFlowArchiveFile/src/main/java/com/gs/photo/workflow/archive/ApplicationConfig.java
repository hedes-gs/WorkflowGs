package com.gs.photo.workflow.archive;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.workflow.model.files.FileToProcess;

@org.springframework.context.annotation.Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    private static Logger      LOGGER                             = LoggerFactory.getLogger(ApplicationConfig.class);

    public final static String KAFKA_FILE_TO_PROCESS_DESERIALIZER = FileToProcessDeserializer.class.getName();

    @Value("${application.gs.principal}")
    String                     principal;
    @Value("${application.gs.keytab}")
    String                     keyTab;

    @Bean(name = "consumerForTransactionalReadOfFileToProcess")
    @Scope("prototype")
    public Consumer<String, FileToProcess> consumerForTransactionalReadOfFileToProcess(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId,
        @Value("${kafka.consumer.batchRecords}") int consumerBatch,
        @Value("${kafka.pollTimeInMillisecondes}") int pollTimeInMillisecondes

    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ApplicationConfig.KAFKA_FILE_TO_PROCESS_DESERIALIZER);
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, "tr-" + this.applicationId);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        Consumer<String, FileToProcess> consumer = new KafkaConsumer<>(settings);
        return consumer;
    }

    @Bean(name = "hdfsFileSystem")
    @Scope("prototype")
    public FileSystem hdfsFileSystem(

    ) {
        Configuration configuration = new Configuration();
        ApplicationConfig.LOGGER.info("Hadoop FileSystem configuration {}", configuration);
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab(this.principal, this.keyTab);
            ApplicationConfig.LOGGER.info("Kerberos Login from login {} and keytab {}", this.principal, this.keyTab);
        } catch (IOException e1) {
            ApplicationConfig.LOGGER
                .warn("Error when login {},{} : {}", this.principal, this.keyTab, ExceptionUtils.getStackTrace(e1));
            throw new RuntimeException(e1);
        }
        PrivilegedAction<FileSystem> action = () -> {
            FileSystem retValue = null;
            try {
                retValue = FileSystem.get(configuration);
            } catch (IOException e) {
                ApplicationConfig.LOGGER.warn(
                    "Error when Getting FileSystem {},{} : {}",
                    this.principal,
                    this.keyTab,
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
                .warn("Error when login {},{} : {}", this.principal, this.keyTab, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Bean(name = "userGroupInformationAction")
    public IUserGroupInformationAction userGroupInformationAction() {
        return new IUserGroupInformationAction() {
            @Override
            public <T> T run(PrivilegedAction<T> action) throws IOException {
                try {
                    return UserGroupInformation.getLoginUser()
                        .doAs(action);
                } catch (IOException e) {
                    ApplicationConfig.LOGGER
                        .warn("Error when running action {}, {} ", action, ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            }
        };
    }
}