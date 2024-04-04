package com.gs.photo.workflow.cmphashkey;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.cache.configuration.Factory;

import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.yaml.snakeyaml.Yaml;

import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.AbstractApplicationKafkaProperties;
import com.gs.photo.common.workflow.IIgniteProperties;
import com.gs.photo.common.workflow.IKafkaConsumerFactory;
import com.gs.photo.common.workflow.IKafkaProducerFactory;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.common.workflow.ports.IIgniteCacheFactory;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.common.workflow.ports.IgniteCacheFactory;
import com.gs.photo.common.workflow.ports.IgniteDAO;
import com.gs.photo.workflow.cmphashkey.ports.IFileUtils;
import com.gs.photo.workflow.cmphashkey.ports.IProcessInputForHashKeyCompute;
import com.workflow.model.HbaseData;
import com.workflow.model.files.FileToProcess;

@Configuration
@EnableAutoConfiguration
@AutoConfigureBefore(IgniteAutoConfiguration.class)
public class ApplicationConfig extends AbstractApplicationConfig {

    public static final String CONSUMER_NAME = "file-to-process";

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
    public IFileUtils fileUtils() { return file -> FileUtils.readFirstBytesOfFileRetry(file); }

    @Bean
    public Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue(
        IKafkaConsumerFactory<String, FileToProcess> defaultKafkaConsumerFactory,
        Map<String, KafkaClientConsumer> kafkaClientConsumers
    ) {
        return () -> defaultKafkaConsumerFactory.get(
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_NAME)
                .consumerType(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_NAME)
                .groupId(),
            kafkaClientConsumers.get(ApplicationConfig.CONSUMER_NAME)
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
    public IIgniteDAO igniteDAO(IIgniteCacheFactory igniteCacheFactory, IgniteConfiguration igniteConfiguration) {
        return new IgniteDAO(igniteCacheFactory);
    }

    @Bean
    public IIgniteCacheFactory igniteCacheFactory(Ignite beanIgnite, IIgniteProperties igniteProperties) {
        return new IgniteCacheFactory(beanIgnite, igniteProperties);
    }

    @Bean
    public Ignite ignite(IgniteConfiguration cfg) { return Ignition.start(cfg); }

    @Bean
    @ConfigurationProperties(prefix = IgniteAutoConfiguration.IGNITE_PROPS_PREFIX)
    public IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        final TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder());
        cfg.setDiscoverySpi(discoSpi);
        cfg.setCommunicationSpi(new TcpCommunicationSpi());
        return cfg;
    }

    @Bean
    @ConfigurationProperties(prefix = "kafka-consumers", ignoreUnknownFields = false)
    public Map<String, KafkaClientConsumer> kafkaClientConsumers() { return new HashMap<>(); }

    @Bean
    @ConfigurationProperties(prefix = "kafka", ignoreUnknownFields = false)
    public IKafkaProperties kafkaProperties() { return new AbstractApplicationKafkaProperties() {}; }

    public class StringToFactoryConverter implements Converter<String, Factory<?>> {

        @Override
        public Factory<EvictionPolicy<?, ?>> convert(String from) {
            switch (from) {
                case "LRU":
                    return () -> new LruEvictionPolicy<>();
                default: {
                    Yaml yaml = new Yaml();
                    Map<String, Object> data = yaml.load(IOUtils.toInputStream(from, Charset.forName("UTF-8")));
                    return () -> new LruEvictionPolicy<>().setMaxSize((Integer) data.get("maxSize"));
                }
            }
        }
    }

    @Bean
    @ConfigurationPropertiesBinding
    public StringToFactoryConverter converter() { return new StringToFactoryConverter(); }

    @Bean
    public Void startConsumers(IProcessInputForHashKeyCompute bean) {
        bean.start();
        return null;
    }

}
