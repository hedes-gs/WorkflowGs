package com.gsphotos.storms.translator;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;

public class KeyValueTranslator implements RecordTranslator<String, Bytes> {

	private static final String       STREAM_EXTRACT_HISTOGRAM = "extractHistogram";
	private static final List<String> STREAMS_LIST             = Arrays
		.asList(KeyValueTranslator.STREAM_EXTRACT_HISTOGRAM);
	/**
	 *
	 */
	private static final long         serialVersionUID         = 1L;

	@Override
	public List<Object> apply(ConsumerRecord<String, Bytes> record) {
		String keyString = record.key();
		Bytes valueAsByte = record.value();
		return new KafkaTuple(keyString, valueAsByte.get()).routedTo(KeyValueTranslator.STREAM_EXTRACT_HISTOGRAM);
	}

	@Override
	public Fields getFieldsFor(String stream) {
		return new Fields("KEY", "VALUE");
	}

	@Override
	public List<String> streams() {
		return KeyValueTranslator.STREAMS_LIST;
	}

}
