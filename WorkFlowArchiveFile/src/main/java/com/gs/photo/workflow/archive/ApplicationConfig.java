package com.gs.photo.workflow.archive;

import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.AbstractApplicationKafkaProperties;
import com.gs.photo.common.workflow.IKafkaConsumerFactory;
import com.gs.photo.common.workflow.IKafkaProducerFactory;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.archive.config.SpecificApplicationProperties;
import com.gs.photo.workflow.archive.ports.IBeanFileConsumer;
import com.gs.photo.workflow.archive.ports.IFileSystem;
import com.gs.photo.workflow.archive.ports.IFileUtils;
import com.gs.photo.workflow.archive.ports.IUserGroupInformationAction;
import com.gs.photos.serializers.FileToProcessDeserializer;
import com.workflow.model.HbaseData;
import com.workflow.model.files.FileToProcess;

@org.springframework.context.annotation.Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {
    public static final String CONSUMER_TYPE_NAME                 = "file-to-process";

    private static Logger      LOGGER                             = LoggerFactory.getLogger(ApplicationConfig.class);

    public final static String KAFKA_FILE_TO_PROCESS_DESERIALIZER = FileToProcessDeserializer.class.getName();

    @Override
    @Bean
    public <K, V> IKafkaProducerFactory<K, V> kafkaProducerFactory(IKafkaProperties kafkaProperties) {
        return super.kafkaProducerFactory(kafkaProperties);
    }

    @Override
    @Bean
    public <K, V> IKafkaConsumerFactory<K, V> kafkaConsumerFactory(IKafkaProperties kafkaProperties) {
        return super.kafkaConsumerFactory(kafkaProperties);
    }

    @Bean
    public IFileUtils fileUtils() {
        return new IFileUtils() {

            @Override
            public void copyRemoteToLocal(FileToProcess value, OutputStream fdsOs) throws IOException {
                FileUtils.copyRemoteToLocal(value, fdsOs);
            }

            @Override
            public boolean deleteIfLocal(FileToProcess value, String root) throws IOException {
                return FileUtils.deleteIfLocal(value, root);
            }

            @Override
            public String getSimpleNameFromUrl(String url) { // TODO Auto-generated method stub
                return FileUtils.getSimpleNameFromUrl(url);
            }
        };
    }

    @Bean
    public Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue(
        IKafkaConsumerFactory<String, FileToProcess> defaultKafkaConsumerFactory,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_TYPE_NAME)
                .consumerType(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_TYPE_NAME)
                .groupId(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_TYPE_NAME)
                .instanceGroupId(),
            AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER,
            AbstractApplicationConfig.KAFKA_FILE_TO_PROCESS_DESERIALIZER);
    }

    @Bean
    public Supplier<Producer<String, HbaseData>> producerSupplierForTransactionPublishingOnExifTopic(
        IKafkaProducerFactory<String, HbaseData> defaultKafkaProducerFactory
    ) {
        return () -> defaultKafkaProducerFactory.get(
            AbstractApplicationConfig.MEDIUM_PRODUCER_TYPE,
            AbstractApplicationConfig.KAFKA_STRING_SERIALIZER,
            AbstractApplicationConfig.KAFKA_MULTIPLE_SERIALIZER);
    }

    @Bean(name = "hdfsFileSystem")
    public Supplier<IFileSystem> supplierForHdfsFileSystem(
        SpecificApplicationProperties specificApplicationProperties
    ) {
        return () -> {
            Configuration configuration = new Configuration();
            ApplicationConfig.LOGGER.info("Hadoop FileSystem configuration {}", configuration);
            UserGroupInformation.setConfiguration(configuration);
            try {
                UserGroupInformation.loginUserFromKeytab(
                    specificApplicationProperties.getHadoopPrincipal(),
                    specificApplicationProperties.getHadoopKeyTab());
                ApplicationConfig.LOGGER.info(
                    "Kerberos Login from login {} and keytab {}",
                    specificApplicationProperties.getHadoopPrincipal(),
                    specificApplicationProperties.getHadoopKeyTab());
            } catch (IOException e1) {
                ApplicationConfig.LOGGER.warn(
                    "Error when login {},{} : {}",
                    specificApplicationProperties.getHadoopPrincipal(),
                    specificApplicationProperties.getHadoopKeyTab(),
                    ExceptionUtils.getStackTrace(e1));
                throw new RuntimeException(e1);
            }
            PrivilegedAction<FileSystem> action = () -> {
                FileSystem retValue = null;
                try {
                    retValue = FileSystem.get(configuration);
                } catch (IOException e) {
                    ApplicationConfig.LOGGER.warn(
                        "Error when Getting FileSystem {},{} : {}",
                        specificApplicationProperties.getHadoopPrincipal(),
                        specificApplicationProperties.getHadoopKeyTab(),
                        ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
                return retValue;
            };
            try {
                FileSystem returnValue = UserGroupInformation.getLoginUser()
                    .doAs(action);
                return new IFileSystem() {

                    @Override
                    public void close() throws IOException { returnValue.close(); }

                    @Override
                    public boolean mkdirs(Path folderWhereRecord) throws IOException {
                        PrivilegedAction<Boolean> action = () -> {
                            try {
                                return returnValue.mkdirs(folderWhereRecord);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        };
                        return UserGroupInformation.getLoginUser()
                            .doAs(action);
                    }

                    @Override
                    public boolean exists(Path hdfsFilePath) throws IOException {
                        PrivilegedAction<Boolean> action = () -> {
                            try {
                                return returnValue.exists(hdfsFilePath);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        };
                        return UserGroupInformation.getLoginUser()
                            .doAs(action);
                    }

                    @Override
                    public OutputStream create(Path hdfsFilePath, boolean b) throws IOException {
                        PrivilegedAction<OutputStream> action = () -> {
                            try {
                                return returnValue.create(hdfsFilePath, b);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        };
                        return UserGroupInformation.getLoginUser()
                            .doAs(action);
                    }
                };
            } catch (IOException e) {
                ApplicationConfig.LOGGER.warn(
                    "Error when login {},{} : {}",
                    specificApplicationProperties.getHadoopPrincipal(),
                    specificApplicationProperties.getHadoopKeyTab(),
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        };
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

    @Bean(name = "threadPoolTaskExecutor")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setDaemon(true);
        threadPoolTaskExecutor.setCorePoolSize(6);
        threadPoolTaskExecutor.setMaxPoolSize(64);
        threadPoolTaskExecutor.setThreadNamePrefix("wf-task-executor");
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    @Bean
    public Void startConsumers(IBeanFileConsumer bean) {
        bean.start();
        return null;
    }

    @Bean
    @ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = false)
    public IKafkaProperties kafkaProperties() { return new AbstractApplicationKafkaProperties() {}; }

    @Bean
    @ConfigurationProperties(prefix = "kafka-consumers", ignoreUnknownFields = false)
    public Map<String, KafkaClientConsumer> kafkaClientConsumers() { return new HashMap<>(); }

    @Bean
    @ConfigurationProperties(prefix = AbstractApplicationConfig.CONFIG_PREIFX_APPLICATION_SPECIFIC, ignoreUnknownFields = false)
    public SpecificApplicationProperties specificApplicationProperties() { return new SpecificApplicationProperties(); }

    @Bean
    String nameSpace(SpecificApplicationProperties specificApplicationProperties) {
        return specificApplicationProperties.getNameSpace();
    }

}