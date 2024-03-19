package com.gs.photo.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    public static final String SEED_ID = "-for-checkout";

    @Bean
    public Map<String, Object> consumerConfigsForTopicCheckout(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + KafkaConsumerConfig.SEED_ID);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        return config;
    }

    @Bean
    public Map<String, Object> consumerConfigsForTopicUpdate(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        return config;
    }

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        return new DefaultKafkaConsumerFactory<>(
            this.consumerConfigsForTopicCheckout(bootstrapServer, sessionTimeoutMs, groupId));
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactoryForTopicUpdate(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        return new DefaultKafkaConsumerFactory<>(
            this.consumerConfigsForTopicUpdate(bootstrapServer, sessionTimeoutMs, groupId));
    }

    @Bean("KafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, byte[]>> kafkaListenerContainerFactory(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactory(bootstrapServer, sessionTimeoutMs, groupId));
        return factory;

    }

    @Bean("kafkaListenerContainerFactoryForTopicUpdate")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactoryForTopicUpdate(
        @Value("${bootstrap.servers}") String bootstrapServer,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${group.id}") String groupId
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.consumerFactoryForTopicUpdate(bootstrapServer, sessionTimeoutMs, groupId));
        return factory;

    }

    @Bean
    public ProducerFactory<String, String> producerFactory(@Value("${bootstrap.servers}") String bootstrapServer) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        configProps.put("sasl.kerberos.service.name", "kafka");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateForTopicUpdate(
        @Value("${bootstrap.servers}") String bootstrapServer
    ) {
        return new KafkaTemplate<String, String>(this.producerFactory(bootstrapServer));
    }

    @PostConstruct
    protected void init() {

    }
}