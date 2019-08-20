package com.gs.photo.workflow.kstreams;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.impl.StreamsHelper;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.storm.FinalImage;

@SuppressWarnings("unchecked")
@Component
public class ProcessAndPublishImages implements IProcessAndPublishImages {

	private static final short EXIF_CREATION_DATE_ID = (short) 0x9003;
	private final DateTimeFormatter FORMATTER_FOR_CREATION_DATE = DateTimeFormatter.ofPattern(
		"yyyy:MM:dd HH:mm:ss");

	@Autowired
	protected StreamsHelper streamsHelper;

	@Autowired
	private Properties kafkaStreamProperties;

	@Override
	public KafkaStreams buildKafkaStreamsTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		KTable<String, String> imageKTable = streamsHelper.buildKTableToStoreCreatedImages(
			builder);
		KTable<String, String> pathKTable = streamsHelper.buildKTableToGetPathValue(
			builder);
		KStream<String, FinalImage> thumbImages = streamsHelper.buildKStreamToGetThumbImages(
			builder);
		thumbImages.ma
		KStream<String, ExchangedTiffData> exifOfImageStream = streamsHelper.buildKStreamToGetExifValue(
			builder);

		KStream<String, ExchangedTiffData> filteredImageKStreamForCreationDate = exifOfImageStream.filter(
			(key, exif) -> {
				boolean b = exif.getTag() == EXIF_CREATION_DATE_ID;
				return b;
			}).map(
				(k, v) -> {
					return new KeyValue<String, ExchangedTiffData>(v.getImageId(), v);
				});

		KStream<String, HbaseImageThumbnail> jointureToFindTheCreationDate = filteredImageKStreamForCreationDate.join(
			imageKTable,
			new ValueJoiner<ExchangedTiffData, String, HbaseImageThumbnail>() {

				private HbaseImageThumbnail buildHBaseImageThumbnail(ExchangedTiffData key) {
					HbaseImageThumbnail retValue = new HbaseImageThumbnail();
					retValue.setImageId(
						key.getImageId());
					LocalDateTime localDate = LocalDateTime.parse(
						new String(key.getDataAsByte()).trim(),
						FORMATTER_FOR_CREATION_DATE);
					ZonedDateTime zdt = ZonedDateTime.of(
						localDate,
						ZoneId.of(
							"Europe/Paris"));
					retValue.setCreationDate(
						zdt.toEpochSecond() * 1000);
					return retValue;
				}

				@Override
				public HbaseImageThumbnail apply(ExchangedTiffData key, String value2) {
					return buildHBaseImageThumbnail(
						key);
				}

			},
			Joined.with(
				Serdes.String(),
				new ExchangedDataSerDe(),
				Serdes.String()));

		KStream<String, Long> wordCountsStream = jointureToFindTheCreationDate.flatMap(
			new KeyValueMapper<String, HbaseImageThumbnail, Iterable<? extends KeyValue<String, Long>>>() {

				@Override
				public Iterable<? extends KeyValue<String, Long>> apply(String key, HbaseImageThumbnail value) {
					return splitCreationDateToYearMonthDayAndHour(
						value);
				}

				private Iterable<? extends KeyValue<String, Long>> splitCreationDateToYearMonthDayAndHour(
						HbaseImageThumbnail value) {
					LocalDateTime ldt = LocalDateTime.ofEpochSecond(
						value.getCreationDate() / 1000,
						0,
						ZoneOffset.ofTotalSeconds(
							2 * 60 * 60));
					List<KeyValue<String, Long>> retValue;
					String keyYear = "Y:" + (long) ldt.getYear();
					String keyMonth = keyYear + ";M:" + (long) ldt.getMonthValue();
					String keyDay = keyMonth + ";D:" + (long) ldt.getDayOfMonth();
					String keyHour = keyDay + ";H:" + (long) ldt.getHour();
					String keyMinute = keyHour + ";Mn:" + (long) ldt.getMinute();
					String keySeconde = keyMinute + ";S:" + (long) ldt.getSecond();
					retValue = Arrays.asList(
						new KeyValue<String, Long>(keyYear, 1L),
						new KeyValue<String, Long>(keyMonth, 1L),
						new KeyValue<String, Long>(keyDay, 1L),
						new KeyValue<String, Long>(keyHour, 1L),
						new KeyValue<String, Long>(keyMinute, 1L),
						new KeyValue<String, Long>(keySeconde, 1L));
					return retValue;
				}
			});
		KTable<String, Long> wordCounts = wordCountsStream.groupBy(
			(key, value) -> key,
			Serialized.with(
				Serdes.String(),
				Serdes.Long())).count(
					Materialized.with(
						Serdes.String(),
						Serdes.Long()));
		wordCounts.print(
			"count by day");

		KTable<String, HbaseImageThumbnail> kTableHbaseImageThumbnail = jointureToFindTheCreationDate.groupByKey()
				.reduce(
					(aggValue, newValue) -> newValue,
					Materialized.with(
						Serdes.String(),
						new HbaseImageThumbnailSerDe()));

		KStream<String, HbaseImageThumbnail> finalStream = thumbImages.join(
			kTableHbaseImageThumbnail,
			(key, value) -> {
				HbaseImageThumbnail retValue = null;
				if (value != null) {
					retValue = value;
					retValue.setThumbnail(
						key.getCompressedImage());
					retValue.setHeight(
						key.getHeight());
					retValue.setWidth(
						key.getWidth());
					retValue.setOrignal(
						key.isOriginal());
				}
				return retValue;
			},
			Joined.with(
				Serdes.String(),
				new FinalImageSerDe(),
				new HbaseImageThumbnailSerDe()));

		KStream<String, HbaseImageThumbnail> final2Stream = finalStream.join(
			pathKTable,
			(key, value) -> {
				key.setPath(
					value);
				return key;
			});

		streamsHelper.publishImageDataInRecordTopic(
			final2Stream);
		final KafkaStreams streams = new KafkaStreams(builder.build(), kafkaStreamProperties);

		return streams;

	}

}
