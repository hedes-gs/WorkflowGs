package com.gsphotos.storms.translator;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;

public class KeyValueTranslator implements RecordTranslator<String, Bytes> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public List<Object> apply(ConsumerRecord<String, Bytes> record) {
		String keyString = record.key();
		Bytes valueAsByte = record.value();
		return new Values(
			ImmutableMap.of(
				keyString,
				valueAsByte.get()));
	}

	@Override
	public Fields getFieldsFor(String stream) {
		return new Fields("KEY", "VALUE");
	}

}
