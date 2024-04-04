package com.gs.photo.workflow.dupcheck.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.common.workflow.IStream;
import com.gs.photo.workflow.dupcheck.IDuplicateCheck;
import com.gs.photos.serializers.FileToProcessSerDe;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanDuplicateCheck implements IDuplicateCheck, IStream {

    protected static Logger          LOGGER = LoggerFactory.getLogger(IDuplicateCheck.class);

    @Autowired
    @Qualifier("kafkaStreamTopologyProperties")
    protected Properties             kafkaStreamTopologyProperties;

    @Autowired
    protected IKafkaStreamProperties applicationSpecificProperties;

    @Override
    public Topology buildKafkaStreamsTopology() {
        // How long we "remember" an event. During this time, any incoming duplicates of
        // the event
        // will be, well, dropped, thereby de-duplicating the input data.
        //
        // The actual value depends on your use case. To reduce memory and disk usage,
        // you could
        // decrease the size to purge old windows more frequently at the cost of
        // potentially missing out
        // on de-duplicating late-arriving records.
        long maintainDurationPerEventInMs = TimeUnit.DAYS.toMillis(30);

        // The number of segments has no impact on "correctness".
        // Using more segments implies larger overhead but allows for more fined grained
        // record expiration
        // Note: the specified retention time is a _minimum_ time span and no strict
        // upper time bound
        int numberOfSegments = 3;

        // retention period must be at least window size -- for this use case, we don't
        // need a longer retention period
        // and thus just use the window size as retention time
        long retentionPeriod = maintainDurationPerEventInMs;
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                this.applicationSpecificProperties.getStoreName(),
                Duration.ofMillis(retentionPeriod),
                Duration.ofMillis(maintainDurationPerEventInMs),
                false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        KStream<String, FileToProcess> input = builder.stream(
            this.applicationSpecificProperties.getTopics()
                .topicFileHashkey(),
            Consumed.with(Serdes.String(), new FileToProcessSerDe()));
        KStream<String, FileToProcess> deduplicated = input.transform(
            () -> this.buildDedupTransformer(maintainDurationPerEventInMs),
            this.applicationSpecificProperties.getStoreName());
        deduplicated.filter((K, V) -> { return K.startsWith("DUP-"); })
            .to(
                this.applicationSpecificProperties.getTopics()
                    .topicDuplicateKeyImageFound(),
                Produced.with(Serdes.String(), new FileToProcessSerDe()));
        deduplicated.filterNot((K, V) -> { return K.startsWith("DUP-"); })
            .to(
                this.applicationSpecificProperties.getTopics()
                    .topicDupFilteredFile(),
                Produced.with(Serdes.String(), new FileToProcessSerDe()));

        return builder.build();
    }

    protected DeduplicationTransformer<String, FileToProcess, String> buildDedupTransformer(
        long maintainDurationPerEventInMs
    ) {
        return new DeduplicationTransformer<String, FileToProcess, String>(maintainDurationPerEventInMs,
            (key, value) -> key,
            this.applicationSpecificProperties.getStoreName()) {
            @Override
            public KeyValue<String, FileToProcess> transform(final String key, final FileToProcess value) {
                KeyValue<String, FileToProcess> output = super.transform(key, value);
                if (output == null) {
                    BeanDuplicateCheck.LOGGER.warn("[EVENT][{}]!!! Duplicate value found : {}", key, value);
                    output = KeyValue.pair("DUP-" + key, value);
                } else {
                    BeanDuplicateCheck.LOGGER
                        .info("[EVENT][{}][DUPLICATE]Unique value found FileToProcess value {}", key, value.toString());
                }
                return output;
            }
        };
    }
}
