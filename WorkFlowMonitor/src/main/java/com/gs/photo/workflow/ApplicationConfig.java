package com.gs.photo.workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.gs.photo.ks.transformers.AdvancedMapCollector;
import com.gs.photos.serializers.FileToProcessSerDe;
import com.gs.photos.serializers.WfEventsDeserializer;
import com.workflow.model.CollectionOfExchangedTiffData;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {

    public static final int   JOIN_WINDOW_TIME      = 86400;
    public static final short EXIF_CREATION_DATE_ID = (short) 0x9003;
    public static final short EXIF_SIZE_WIDTH       = (short) 0xA002;
    public static final short EXIF_SIZE_HEIGHT      = (short) 0xA003;

    @Bean(name = "dataSource")
    public DataSource getDataSource(
        @Value("${wf.spring.datasource.driver-class-name}") String driverClassName,
        @Value("${wf.spring.datasource.url}") String dataSourceURl,
        @Value("${wf.spring.datasource.username}") String dataSourceUsername,
        @Value("${wf.spring.datasource.password}") String dataSourcePwd
    ) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        // See: application.properties
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(dataSourceURl);
        dataSource.setUsername(dataSourceUsername);
        dataSource.setPassword(dataSourcePwd);
        return dataSource;
    }

    @Bean(name = "transactionManager")
    public DataSourceTransactionManager getTransactionManager(DataSource dataSource) {
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        return transactionManager;
    }

    @Bean
    @Profile("!test")
    public Consumer<String, WfEvents> consumerOfWfEventsWithStringKey(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${group.id}") String groupId,
        @Value("${application.id}") String applicationId
    ) {
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WfEventsDeserializer.class.getName());
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, this.applicationId);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        Consumer<String, WfEvents> consumer = new KafkaConsumer<>(settings);
        return consumer;

    }

    @Bean
    public Topology kafkaStreamsTopology(
        @Value("${topic.topicEvent}") String topicEvent,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicDupFilteredFile}") String topicDupFilteredFile

    ) {
        AbstractApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {)");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FileToProcess> pathOfImageKStream = this
            .buildKTableToStoreCreatedImages(builder, topicDupFilteredFile);
        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                "store-for-collection-of-tiffdata",
                Duration.ofMillis(10000),
                Duration.ofMillis(10000),
                false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        Transformer<String, ExchangedTiffData, KeyValue<String, CollectionOfExchangedTiffData>> transfomer = AdvancedMapCollector
            .of(
                (k, v) -> new CollectionOfExchangedTiffData(k, 0, new ArrayList<>(v)),
                (k, values) -> AbstractApplicationConfig.LOGGER
                    .info("[EVENT][{}] processed found all elements {} ", k, values.size()),
                (k, values) -> this.isComplete(k, values));
        return null;
    }

    protected KStream<String, FileToProcess> buildKTableToStoreCreatedImages(
        StreamsBuilder builder,
        String topicDupFilteredFile
    ) {
        AbstractApplicationConfig.LOGGER
            .info("building ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);
        return builder.stream(topicDupFilteredFile, Consumed.with(Serdes.String(), new FileToProcessSerDe()));
    }

}
