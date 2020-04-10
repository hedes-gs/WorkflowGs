package com.gs.photo.workflow;

import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.gs.photos.serializers.WfEventsDeserializer;
import com.workflow.model.events.WfEvents;

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
        settings.put(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            IsolationLevel.READ_COMMITTED.toString()
                .toLowerCase(Locale.ROOT));
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        Consumer<String, WfEvents> consumer = new KafkaConsumer<>(settings);
        return consumer;

    }
}
