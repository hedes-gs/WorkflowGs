package com.gs.photo.workflow.scan;

import java.io.IOException;
import java.net.InetAddress;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.IKafkaConsumerFactory;
import com.gs.photo.common.workflow.IKafkaProducerFactory;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.workflow.scan.config.SpecificApplicationProperties;
import com.workflow.model.events.ComponentEvent.ComponentType;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    protected static final org.slf4j.Logger LOGGER                     = LoggerFactory
        .getLogger(ApplicationConfig.class);

    public final static String              FILE_TO_PROCESS_SERIALIZER = com.gs.photos.serializers.FileToProcessSerializer.class
        .getName();

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

    @Bean(name = "threadPoolTaskExecutor")
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        // threadPoolTaskExecutor.setDaemon(false);
        threadPoolTaskExecutor.setCorePoolSize(6);
        threadPoolTaskExecutor.setMaxPoolSize(64);
        threadPoolTaskExecutor.setThreadNamePrefix("wf-task-executor");
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    @Bean
    public Supplier<Consumer<String, ImportEvent>> kafkaConsumerFactoryForImportEvent(
        String createScanName,
        IKafkaConsumerFactory<String, ImportEvent> defaultKafkaConsumerFactory
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            AbstractApplicationConfig.ON_THE_FLY_CONSUMER_TYPE,
            createScanName,
            createScanName,
            AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER,
            AbstractApplicationConfig.KAFKA_IMPORT_EVENT_DESERIALIZER);
    }

    @Bean
    public Supplier<Producer<String, FileToProcess>> producerSupplierForTransactionPublishingOnExifTopic(
        IKafkaProducerFactory<String, FileToProcess> defaultKafkaProducerFactory
    ) {
        return () -> defaultKafkaProducerFactory.get(
            AbstractApplicationConfig.ON_THE_FLY_PRODUCER_TYPE,
            AbstractApplicationConfig.KAFKA_STRING_SERIALIZER,
            AbstractApplicationConfig.KAFKA_FILE_TO_PROCESS_SERIALIZER);
    }

    @Bean
    @ConfigurationProperties(prefix = "application-specific", ignoreUnknownFields = false)
    public SpecificApplicationProperties specificApplicationProperties() { return new SpecificApplicationProperties(); }

    @Bean
    public String createScanName() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            String hostname = ip.getHostName();
            String scanName = hostname + "-" + ComponentType.SCAN;
            return scanName;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Bean
    public Void startConsumers(IScan bean) {
        bean.start();
        return null;
    }

}