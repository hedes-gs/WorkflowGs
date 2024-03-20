package com.gs.photo.workflow.copyfiles;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.IKafkaConsumerFactory;
import com.gs.photo.common.workflow.IKafkaProducerFactory;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.workflow.copyfiles.config.SpecificApplicationProperties;
import com.workflow.model.HbaseData;
import com.workflow.model.files.FileToProcess;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);

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
    public Supplier<Consumer<String, FileToProcess>> consumerSupplierForFileToProcessValue(
        IKafkaConsumerFactory<String, FileToProcess> defaultKafkaConsumerFactory,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            kafkaClientConsumers.get("file-to-process")
                .consumerType(),
            kafkaClientConsumers.get("file-to-process")
                .groupId(),
            kafkaClientConsumers.get("file-to-process")
                .instanceGroupId(),
            AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER,
            AbstractApplicationConfig.KAFKA_FILE_TO_PROCESS_DESERIALIZER);
    }

    @Bean
    public Supplier<Producer<String, HbaseData>> producerSupplierForTransactionTopicWithFileToProcessOrEventValue(
        IKafkaProducerFactory<String, HbaseData> defaultKafkaProducerFactory
    ) {
        return () -> defaultKafkaProducerFactory.get(
            AbstractApplicationConfig.ON_THE_FLY_PRODUCER_TYPE,
            AbstractApplicationConfig.KAFKA_STRING_SERIALIZER,
            AbstractApplicationConfig.KAFKA_MULTIPLE_SERIALIZER);
    }

    @Bean
    @ConfigurationProperties(prefix = "application-specific")
    public SpecificApplicationProperties specificApplicationProperties() { return new SpecificApplicationProperties(); }

}
