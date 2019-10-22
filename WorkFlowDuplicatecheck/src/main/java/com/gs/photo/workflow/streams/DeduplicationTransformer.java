package com.gs.photo.workflow.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

	private ProcessorContext              context;

	/**
	 * Key: event ID Value: timestamp (event-time) of the corresponding event when
	 * the event ID was seen for the first time
	 */
	private WindowStore<E, Long>          eventIdStore;

	private final long                    leftDurationMs;
	private final long                    rightDurationMs;
	private String                        storeName;
	private final KeyValueMapper<K, V, E> idExtractor;

	/**
	 * @param maintainDurationPerEventInMs
	 *            how long to "remember" a known event (or rather, an event ID),
	 *            during the time of which any incoming duplicates of the event will
	 *            be dropped, thereby de-duplicating the input.
	 * @param idExtractor
	 *            extracts a unique identifier from a record by which we
	 *            de-duplicate input records; if it returns null, the record will
	 *            not be considered for de-duping but forwarded as-is.
	 * @param storeName
	 *            TODO
	 */
	public DeduplicationTransformer(
			long maintainDurationPerEventInMs,
			KeyValueMapper<K, V, E> idExtractor,
			String storeName) {
		if (maintainDurationPerEventInMs < 1) {
			throw new IllegalArgumentException("maintain duration per event must be >= 1");
		}
		this.leftDurationMs = maintainDurationPerEventInMs / 2;
		this.rightDurationMs = maintainDurationPerEventInMs - this.leftDurationMs;
		this.idExtractor = idExtractor;
		this.storeName = storeName;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void init(final ProcessorContext context) {
		this.context = context;
		this.eventIdStore = (WindowStore<E, Long>) context.getStateStore(this.storeName);
	}

	@Override
	public KeyValue<K, V> transform(final K key, final V value) {
		E eventId = this.idExtractor.apply(key,
				value);
		if (eventId == null) {
			return KeyValue.pair(key,
					value);
		} else {
			KeyValue<K, V> output;
			if (this.isDuplicate(eventId)) {
				output = null;
				this.updateTimestampOfExistingEventToPreventExpiry(eventId,
						this.context.timestamp());
			} else {
				output = KeyValue.pair(key,
						value);
				this.rememberNewEvent(eventId,
						this.context.timestamp());
			}
			return output;
		}
	}

	private boolean isDuplicate(final E eventId) {
		long eventTime = this.context.timestamp();
		WindowStoreIterator<Long> timeIterator = this.eventIdStore.fetch(eventId,
				eventTime - this.leftDurationMs,
				eventTime + this.rightDurationMs);
		boolean isDuplicate = timeIterator.hasNext();
		timeIterator.close();
		return isDuplicate;
	}

	private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, long newTimestamp) {
		this.eventIdStore.put(eventId,
				newTimestamp,
				newTimestamp);
	}

	private void rememberNewEvent(final E eventId, long timestamp) {
		this.eventIdStore.put(eventId,
				timestamp,
				timestamp);
	}

	@Override
	public void close() {
		// Note: The store should NOT be closed manually here via
		// `eventIdStore.close()`!
		// The Kafka Streams API will automatically close stores when necessary.
	}

}
