package com.gs.photo.workflow;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;

import com.gs.photos.serializers.HbaseExifOrImageSerializer;

@Configuration
@ImportResource("file:${user.home}/config/cluster-client.xml")
@PropertySource("file:${user.home}/config/application.properties")
public class ApplicationConfig extends AbstractApplicationConfig {

    public static final String KAFKA_EXIF_OR_IMAGE_SERIALIZER = HbaseExifOrImageSerializer.class.getName();

    @Bean("producerForTransactionPublishingOnExifOrImageTopic")
    public Producer<String, Object> producerForTransactionPublishingOnExifOrImageTopic(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ApplicationConfig.KAFKA_EXIF_OR_IMAGE_SERIALIZER);
        settings.put("sasl.kerberos.service.name", "kafka");
        Producer<String, Object> producer = new KafkaProducer<>(settings);
        return producer;
    }

}
