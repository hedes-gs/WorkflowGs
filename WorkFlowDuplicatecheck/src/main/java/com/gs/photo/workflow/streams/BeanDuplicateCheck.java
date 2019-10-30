package com.gs.photo.workflow.streams;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.AbstractStream;
import com.gs.photo.workflow.IDuplicateCheck;
import com.gs.photo.workflow.IStreamsHelper;

@Component
public class BeanDuplicateCheck extends AbstractStream implements IDuplicateCheck {

	protected static Logger  LOGGER = LoggerFactory.getLogger(IDuplicateCheck.class);

	@Autowired
	@Qualifier("kafkaStreamProperties")
	protected Properties     kafkaStreamProperties;

	@Value("${duplicate.storeName}")
	protected String         storeName;

	@Value("${topic.topicFileHashKey}")
	protected String         topicFileHashKey;

	@Value("${topic.duplicateImageFoundTopic}")
	protected String         duplicateImageFoundTopic;

	@Value("${topic.topicDupFilteredFile}")
	protected String         topicDupFilteredFile;

	@Autowired
	protected IStreamsHelper streamsHelper;

	protected KafkaStreams   streams;

	@Override
	public KafkaStreams buildKafkaStreamsTopology() {
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
				Stores.persistentWindowStore(this.storeName,
						retentionPeriod,
						numberOfSegments,
						maintainDurationPerEventInMs,
						false),
				Serdes.String(),
				Serdes.Long());

		builder.addStateStore(dedupStoreBuilder);

		KStream<String, String> input = builder.stream(this.topicFileHashKey);
		Transformer<String, String, KeyValue<String, String>> duplicationTranformer = new DeduplicationTransformer<String, String, String>(
			maintainDurationPerEventInMs,
			(key, value) -> key,
			this.storeName) {
			@Override
			public KeyValue<String, String> transform(final String key, final String value) {
				KeyValue<String, String> output = super.transform(key,
						value);
				if (output == null) {
					BeanDuplicateCheck.LOGGER.info("[EVENT][{}]!!! Duplicate value found : {}",
							key,
							value);
					output = KeyValue.pair("DUP-" + key,
							value);
				} else {
					BeanDuplicateCheck.LOGGER.info("Unique value found for key {} and value {}",
							key,
							value);
				}
				return output;
			}
		};
		KStream<String, String> deduplicated = input.transform(() -> duplicationTranformer,
				this.storeName);
		deduplicated.filter((K, V) -> {
			return K.startsWith("DUP-");
		}).to(this.duplicateImageFoundTopic);
		deduplicated.filterNot((K, V) -> {
			return K.startsWith("DUP-");
		}).to(this.topicDupFilteredFile);

		KafkaStreams streams = new KafkaStreams(builder.build(), this.kafkaStreamProperties);
		return streams;
	}

	@PostConstruct
	public void init() {
		this.streams = this.buildKafkaStreamsTopology();
		this.streams.start();
	}

}
