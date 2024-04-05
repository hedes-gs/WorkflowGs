package com.gs.photo.workflow.recinhbase;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.AbstractApplicationKafkaProperties;
import com.gs.photo.common.workflow.IKafkaConsumerFactory;
import com.gs.photo.common.workflow.IKafkaProducerFactory;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.workflow.recinhbase.business.IProcessKafkaEvent;
import com.gs.photo.workflow.recinhbase.business.ProcessKafkaEvent;
import com.gs.photo.workflow.recinhbase.consumers.GenericConsumerForRecording;
import com.gs.photo.workflow.recinhbase.consumers.IGenericConsumerForRecording;
import com.gs.photo.workflow.recinhbase.consumers.config.SpecificApplicationProperties;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEvents;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {
    private static final String HBASE_MASTER_KERBEROS_PRINCIPAL       = "hbase.master.kerberos.principal";
    private static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
    private static final String HBASE_RPC_PROTECTION                  = "hbase.rpc.protection";
    private static final String HBASE_SECURITY_AUTHENTICATION         = "hbase.security.authentication";
    private static final String HADOOP_SECURITY_AUTHENTICATION        = "hadoop.security.authentication";
    public static final String  CONSUMER_IMAGES                       = "consumer-images";
    public static final String  CONSUMER_EXIF                         = "consumer-exif";

    private static Logger       LOGGER                                = LoggerFactory
        .getLogger(ApplicationConfig.class);

    @Bean
    @ConditionalOnMissingBean
    public org.apache.hadoop.conf.Configuration hbaseConfiguration(
        SpecificApplicationProperties specificApplicationProperties
    ) {

        org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, specificApplicationProperties.getZookeeperHosts());
        hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, specificApplicationProperties.getZookeeperPort());
        hBaseConfig.set(ApplicationConfig.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(ApplicationConfig.HBASE_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(HConstants.CLUSTER_DISTRIBUTED, "true");
        hBaseConfig.set(ApplicationConfig.HBASE_RPC_PROTECTION, "authentication");
        hBaseConfig.set(ApplicationConfig.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");
        hBaseConfig.set(ApplicationConfig.HBASE_MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");

        return hBaseConfig;
    }

    @Bean
    @ConditionalOnMissingBean
    public Connection hbaseConnection(
        @Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
        SpecificApplicationProperties specificApplicationProperties
    ) {
        ApplicationConfig.LOGGER.info("creating the hbase connection");
        try {
            UserGroupInformation.setConfiguration(hbaseConfiguration);
            try {
                UserGroupInformation.loginUserFromKeytab(
                    specificApplicationProperties.getHbasePrincipal(),
                    specificApplicationProperties.getHbaseKeyTable());
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
        } catch (Exception e) {
            ApplicationConfig.LOGGER.warn("unable to create the hbase connection", e);
        }
        return null;
    }

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
    public Supplier<Consumer<String, HbaseImageThumbnail>> kafkaConsumerSupplierForRecordingImage(
        IKafkaConsumerFactory<String, HbaseImageThumbnail> defaultKafkaConsumerFactory,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_IMAGES)
                .consumerType(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_IMAGES)
                .groupId(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_IMAGES)
                .instanceGroupId(),
            AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER,
            AbstractApplicationConfig.HBASE_IMAGE_THUMBNAIL_DESERIALIZER);
    }

    @Bean
    public Supplier<Consumer<String, HbaseExifData>> kafkaConsumerSupplierForRecordingExifImages(
        IKafkaConsumerFactory<String, HbaseExifData> defaultKafkaConsumerFactory,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_EXIF)
                .consumerType(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_EXIF)
                .groupId(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_EXIF)
                .instanceGroupId(),
            AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER,
            AbstractApplicationConfig.HBASE_IMAGE_EXIF_DATA_DESERIALIZER);
    }

    @Bean
    public Supplier<Producer<String, WfEvents>> producerSupplierForTransactionPublishingOnExifTopic(
        IKafkaProducerFactory<String, WfEvents> defaultKafkaProducerFactory
    ) {
        return () -> defaultKafkaProducerFactory.get(
            AbstractApplicationConfig.ON_THE_FLY_PRODUCER_TYPE,
            AbstractApplicationConfig.KAFKA_STRING_SERIALIZER,
            AbstractApplicationConfig.KAFKA_WFEVENTS_SERIALIZER);
    }

    @Bean
    public IProcessKafkaEvent<WfEventRecorded, HbaseExifData> processExifData(
        ProcessKafkaEvent<WfEventRecorded, HbaseExifData> processKafkaEvent
    ) {
        return processKafkaEvent;
    }

    @Bean
    public IProcessKafkaEvent<WfEventRecorded, HbaseImageThumbnail> processImageThumbnail(
        ProcessKafkaEvent<WfEventRecorded, HbaseImageThumbnail> processKafkaEvent
    ) {
        return processKafkaEvent;
    }

    @Bean
    public IGenericConsumerForRecording<WfEvents, HbaseImageThumbnail> consumerForRecordingForHbaseImageThumbnail(
        IProcessKafkaEvent<WfEventRecorded, HbaseImageThumbnail> processImageThumbnail,
        Supplier<Consumer<String, HbaseImageThumbnail>> kafkaConsumerSupplierForRecordingImage,
        Supplier<Producer<String, WfEvents>> producerSupplierForTransactionPublishingOnExifTopic,
        ThreadPoolTaskExecutor threadPoolTaskExecutor,
        SpecificApplicationProperties specificApplicationProperties,
        IKafkaProperties kafkaProperties,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        GenericConsumerForRecording<WfEvents, WfEventRecorded, HbaseImageThumbnail> consumer = GenericConsumerForRecording
            .of(
                HbaseImageThumbnail.class,
                kafkaProperties.getTopics()
                    .topicImageDataToPersist(),
                processImageThumbnail,
                kafkaConsumerSupplierForRecordingImage,
                producerSupplierForTransactionPublishingOnExifTopic,
                threadPoolTaskExecutor,
                specificApplicationProperties,
                kafkaProperties.getConsumersType()
                    .get(
                        kafkaClientConsumers.get(ApplicationConfig.CONSUMER_EXIF)
                            .consumerType()),
                kafkaProperties);
        return consumer;
    }

    @Bean
    public IGenericConsumerForRecording<WfEvents, HbaseExifData> consumerForRecordingForExif(
        IProcessKafkaEvent<WfEventRecorded, HbaseExifData> processExifData,
        Supplier<Consumer<String, HbaseExifData>> kafkaConsumerSupplierForRecordingExifImages,
        Supplier<Producer<String, WfEvents>> producerSupplierForTransactionPublishingOnExifTopic,
        ThreadPoolTaskExecutor threadPoolTaskExecutor,
        SpecificApplicationProperties specificApplicationProperties,
        IKafkaProperties kafkaProperties,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        GenericConsumerForRecording<WfEvents, WfEventRecorded, HbaseExifData> consumer = GenericConsumerForRecording.of(
            HbaseExifData.class,
            kafkaProperties.getTopics()
                .topicExifImageDataToPersist(),
            processExifData,
            kafkaConsumerSupplierForRecordingExifImages,
            producerSupplierForTransactionPublishingOnExifTopic,
            threadPoolTaskExecutor,
            specificApplicationProperties,
            kafkaProperties.getConsumersType()
                .get(
                    kafkaClientConsumers.get(ApplicationConfig.CONSUMER_IMAGES)
                        .consumerType()),
            kafkaProperties);
        return consumer;
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
    public Void startConsumers(List<IGenericConsumerForRecording<?, ?>> allBeans) {
        allBeans.forEach(t -> t.start());
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
    public String nameSpace(SpecificApplicationProperties specificApplicationProperties) {
        return specificApplicationProperties.getNameSpace();
    }

}
