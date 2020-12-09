package com.gs.photo.workflow;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageThumbnail;

@Configuration
@ImportResource("file:${user.home}/config/cluster-client.xml")
public class HbaseApplicationConfig extends AbstractApplicationConfig {
    private static final String HBASE_MASTER_KERBEROS_PRINCIPAL       = "hbase.master.kerberos.principal";
    private static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
    private static final String HBASE_RPC_PROTECTION                  = "hbase.rpc.protection";
    private static final String HBASE_SECURITY_AUTHENTICATION         = "hbase.security.authentication";
    private static final String HADOOP_SECURITY_AUTHENTICATION        = "hadoop.security.authentication";
    public static final String  CONSUMER_IMAGE                        = "consumer-image";
    public static final String  CONSUMER_EXIF                         = "consumer-exif";

    private static Logger       LOGGER                                = LogManager
        .getLogger(HbaseApplicationConfig.class);

    @Bean
    protected org.apache.hadoop.conf.Configuration hbaseConfiguration(
        @Value("${zookeeper.hosts}") String zookeeperHosts,
        @Value("${zookeeper.port}") int zookeeperPort
    ) {

        org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zookeeperHosts);
        hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
        hBaseConfig.set(HbaseApplicationConfig.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(HbaseApplicationConfig.HBASE_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(HConstants.CLUSTER_DISTRIBUTED, "true");
        hBaseConfig.set(HbaseApplicationConfig.HBASE_RPC_PROTECTION, "authentication");
        hBaseConfig.set(HbaseApplicationConfig.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");
        hBaseConfig.set(HbaseApplicationConfig.HBASE_MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");

        return hBaseConfig;
    }

    @Bean
    public Connection hbaseConnection(
        @Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
        @Value("${application.gs.principal}") String principal,
        @Value("${application.gs.keytab}") String keytab
    ) {
        HbaseApplicationConfig.LOGGER.info("creating the hbase connection");
        UserGroupInformation.setConfiguration(hbaseConfiguration);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
        PrivilegedAction<Connection> action = () -> {
            try {
                return ConnectionFactory.createConnection(hbaseConfiguration);
            } catch (IOException e) {
                HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            }
            return null;
        };
        try {
            return UserGroupInformation.getCurrentUser()
                .doAs(action);
        } catch (IOException e) {
            HbaseApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
    }

    @Bean(name = "consumerToRecordExifDataOfImages")
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    @Scope("prototype")
    public Consumer<String, HbaseData> consumerToRecordExifDataOfImages(
        @Value("${application.id}") String applicationId,
        @Value("${group.id}") String groupId,
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${kafka.producer.maxRequestSize}") int producerRequestMaxBytes,
        @Value("${kafka.consumer.batchRecords}") int batchRecords

    ) throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        String hostname = ip.getHostName();
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchRecords);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");

        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.HBASE_DATA_DESERIALIZER);
        settings
            .put(ConsumerConfig.CLIENT_ID_CONFIG, "tr-" + applicationId + "-" + HbaseApplicationConfig.CONSUMER_EXIF);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + HbaseApplicationConfig.CONSUMER_EXIF);
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 30 * 1024 * 1024);
        settings.put(
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            groupId + "-" + HbaseApplicationConfig.CONSUMER_EXIF + "-" + hostname);

        Consumer<String, HbaseData> consumer = new KafkaConsumer<>(settings);
        return consumer;
    }

    /*
     * @Bean(name = "consumerForRecordingImageFromTopicHbaseImageThumbnailKey")
     *
     * @ConditionalOnProperty(name = "unit-test", havingValue = "false")
     * Consumer<String, HbaseImageThumbnailKey>
     * consumerForRecordingImageFromTopicHbaseImageThumbnailKey(
     *
     * @Value("${application.id}") String applicationId,
     *
     * @Value("${bootstrap.servers}") String bootstrapServers,
     *
     * @Value("${group.id}") String groupId,
     *
     * @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes )
     * { Properties settings = new Properties();
     * settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     * settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
     * settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
     * settings.put( ConsumerConfig.ISOLATION_LEVEL_CONFIG,
     * IsolationLevel.READ_COMMITTED.toString() .toLowerCase(Locale.ROOT));
     * settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
     * settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
     * SecurityProtocol.SASL_PLAINTEXT.name);
     * settings.put("sasl.kerberos.service.name", "kafka");
     * settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
     * AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER); settings.put(
     * ConsumerConfig.ISOLATION_LEVEL_CONFIG,
     * IsolationLevel.READ_COMMITTED.toString() .toLowerCase(Locale.ROOT));
     * settings.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
     * AbstractApplicationConfig.HBASE_IMAGE_THUMBNAIL_KEY_DESERIALIZER);
     * settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" +
     * HbaseApplicationConfig.CONSUMER_IMAGE); settings
     * .put(ConsumerConfig.CLIENT_ID_CONFIG, "tr-" + applicationId + "-" +
     * HbaseApplicationConfig.CONSUMER_IMAGE);
     * settings.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
     *
     * Consumer<String, HbaseImageThumbnailKey> consumer = new
     * KafkaConsumer<>(settings); return consumer;
     *
     * }
     */

    @Bean(name = "consumerForRecordingImageFromTopic")
    @ConditionalOnProperty(name = "unit-test", havingValue = "false")
    @Scope("prototype")
    public Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic(
        @Value("${application.id}") String applicationId,
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${group.id}") String groupId,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes
    ) throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        String hostname = ip.getHostName();
        Properties settings = new Properties();
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put("sasl.kerberos.service.name", "kafka");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_DESERIALIZER);
        settings.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            AbstractApplicationConfig.HBASE_IMAGE_THUMBNAIL_DESERIALIZER);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + HbaseApplicationConfig.CONSUMER_IMAGE);
        settings.put(
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            groupId + "-" + HbaseApplicationConfig.CONSUMER_IMAGE + "-" + hostname);

        settings
            .put(ConsumerConfig.CLIENT_ID_CONFIG, "tr-" + applicationId + "-" + HbaseApplicationConfig.CONSUMER_IMAGE);
        settings.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        Consumer<String, HbaseImageThumbnail> consumer = new KafkaConsumer<>(settings);
        return consumer;

    }

    @Bean("propertiesForPublishingWfEvents")
    @Scope("prototype")
    public Properties propertiesForPublishingWfEvents(
        @Value("${transaction.id}") String transactionId,
        @Value("${transaction.timeout}") String transactionTimeout,
        @Value("${bootstrap.servers}") String bootstrapServers
    ) {

        HbaseApplicationConfig.LOGGER.info("creating propertiesForPublishingWfEvents");

        Properties settings = new Properties();
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_STRING_SERIALIZER);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AbstractApplicationConfig.KAFKA_WFEVENTS_SERIALIZER);
        settings.put("sasl.kerberos.service.name", "kafka");
        return settings;
    }

}
