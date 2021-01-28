package com.gs.photo.workflow;

import java.time.Duration;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.WfEventSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {

    private static Logger       LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);

    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

    @Bean
    public Properties kafkaStreamProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
        @Value("${application.kafkastreams.id}") String applicationId,
        @Value("${kafka.pollTimeInMillisecondes}") int pollTimeInMillisecondes,
        @Value("${kafka.consumer.batchRecords}") int consumerBatch,
        @Value("${kafka.stream.commit.interval.ms}") int commitIntervalIms,
        @Value("${kafka.stream.metadata.age.ms}") int metaDataAgeIms,
        @Value("${kafka.stream.nb.of.threads}") int nbOfThreads,
        @Value("${kafka.producer.maxBlockMsConfig}") int maxBlockMsConfig,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes,
        @Value("${kafka.producer.maxRequestSize}") int producerRequestMaxBytes,
        @Value("${kafka.consumer.retryBackoffMsmaxRequestSize}") int retryBackoffMs,
        @Value("${kafka.consumer.reconnectBackoffMs}") int reconnectBackoffMs,
        @Value("${kafka.consumer.heartbeatIntervalMs}") int heartbeatIntervalMs,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${transaction.timeout}") String transactionTimeout

    ) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStreamDir);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalIms);
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metaDataAgeIms);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, nbOfThreads);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestMaxBytes);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        config.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);

        return config;
    }

    @Bean
    public Topology kafkaStreamsTopology(
        @Value("${topic.topicExifSizeOfImageStream}") String topicExifSizeOfImageStream,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicProducedEvent}") String topicEvent,
        @Value("${topic.topicProcessedEvent}") String topicProcessedEvent,
        @Value("${kafkastreams.windowsOfEventsInMs}") int windowsOfEvents,
        @Value("${events.collectorEventsBufferSize}") int collectorEventsBufferSize,
        @Value("${events.collectorEventsTimeWindow}") int collectorEventsTimeWindow
    ) {
        ApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {}", windowsOfEvents);
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, WfEvents> kstreamToGetProducedEvent = this
            .buildKStreamToGetProducedEventsValue(builder, topicEvent);
        final KStream<String, WfEvents> kstreamToGetRecordedEvent = this
            .buildKStreamToGetProducedEventsValue(builder, topicProcessedEvent);

        KStream<String, WfEvent> kstreamOfProducedEvent = kstreamToGetProducedEvent.flatMap(
            (key, value) -> (Iterable<KeyValue<String, WfEvent>>) value.getEvents()
                .stream()
                .map((e) -> this.toKeyValue(e))
                .collect(Collectors.toList())
                .iterator());

        KStream<String, WfEvent> kstream = kstreamToGetRecordedEvent.flatMap(
            (key, value) -> (Iterable<KeyValue<String, WfEvent>>) value.getEvents()
                .stream()
                .map((e) -> new KeyValue(e.getDataId(), e))
                .collect(Collectors.toList())
                .iterator());

        kstreamToGetRecordedEvent.join(
            kstreamToGetProducedEvent,
            (v1, v2) -> v2,
            JoinWindows.of(Duration.ofHours(6)),
            StreamJoined.with(Serdes.String(), new WfEventSerDe(), new WfEventSerDe()));

        return builder.build();
    }

    private KeyValue<String, WfEvent> toKeyValue(WfEvent e) { return null; }

    private KStream<String, WfEvents> buildKStreamToGetProducedEventsValue(
        StreamsBuilder streamsBuilder,
        String topicExif
    ) {
        KStream<String, WfEvent> stream = streamsBuilder
            .stream(topicExif, Consumed.with(Serdes.String(), new WfEventsSerDe()))
            .flatMapValues((v) -> v.getEvents());
        return stream;
    }

    protected void publishImageDataInRecordTopic(
        KStream<String, HbaseData> finalStream,
        String topicExifImageDataToPersist
    ) {
        finalStream.to(topicExifImageDataToPersist, Produced.with(Serdes.String(), new HbaseDataSerDe()));
    }

}
