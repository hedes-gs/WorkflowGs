package com.gs.photo.workflow;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.workflow.model.files.FileToProcess;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
public class ApplicationConfig extends AbstractApplicationConfig {

    public final static String FILE_TO_PROCESS_SERIALIZER = com.gs.photos.serializers.FileToProcessSerializer.class
        .getName();

    @Bean
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    public Producer<String, FileToProcess> producerForPublishingOnFileTopic(
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ApplicationConfig.FILE_TO_PROCESS_SERIALIZER);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        AbstractApplicationConfig.LOGGER.info("creating producer string string with config {} ", settings.toString());
        Producer<String, FileToProcess> producer = new KafkaProducer<>(settings);
        return producer;
    }

}