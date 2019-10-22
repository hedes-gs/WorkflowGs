package com.gs.photo.workflow;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseExifDataSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifData.Builder;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;

@Configuration
@PropertySource("file:${user.home}/config/application.properties")
public class ApplicationConfig extends AbstractApplicationConfig {

	public static final int                JOIN_WINDOW_TIME            = 86400;
	public static final short              EXIF_CREATION_DATE_ID       = (short) 0x9003;
	private static final DateTimeFormatter FORMATTER_FOR_CREATION_DATE = DateTimeFormatter
			.ofPattern("yyyy:MM:dd HH:mm:ss");

	@Bean
	public Properties kafkaStreamProperties() {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG,
				this.applicationGroupId + "-duplicate-streams");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				this.bootstrapServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.String().getClass());
		config.put(StreamsConfig.STATE_DIR_CONFIG,
				this.kafkaStreamDir);
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
				StreamsConfig.EXACTLY_ONCE);
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
				"0");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest");
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
				0);

		return config;
	}

	@Bean
	public Topology kafkaStreamsTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, HbaseExifData> exifOfImageStream = this.buildKStreamToGetExifValue(builder)
				.map((key, v_ExchangedTiffData) -> {
					return new KeyValue<>(key, this.buildHbaseExifData(v_ExchangedTiffData));
				});
		KStream<String, HbaseImageThumbnail> hbaseThumbMailStream = this.buildKStreamToGetHbaseImageThumbNail(builder);
		KStream<String, HbaseExifData> hbaseExifUpdate = exifOfImageStream.join(hbaseThumbMailStream,
				(v_HbaseExifData, v_HbaseImageThumbnail) -> {
					return this.updateHbaseExifData(v_HbaseExifData,
							v_HbaseImageThumbnail);
				},
				JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
				Joined.with(Serdes.String(),
						new HbaseExifDataSerDe(),
						new HbaseImageThumbnailSerDe()));
		KStream<String, HbaseData> finalStream = hbaseExifUpdate.flatMapValues((key, value) -> {
			final Collection<HbaseData> asList = Arrays.asList(value,
					this.buildHbaseExifDataOfImages(value));
			return asList;
		});
		this.publishImageDataInRecordTopic(finalStream);

		return builder.build();

	}

	private HbaseExifData updateHbaseExifData(HbaseExifData v_HbaseExifData,
			HbaseImageThumbnail v_HbaseImageThumbnail) {
		v_HbaseExifData.setCreationDate(v_HbaseImageThumbnail.getCreationDate());
		v_HbaseExifData.setWidth(v_HbaseImageThumbnail.getWidth());
		v_HbaseExifData.setHeight(v_HbaseImageThumbnail.getHeight());
		v_HbaseExifData.setImageId(v_HbaseImageThumbnail.getImageId());
		v_HbaseExifData.setThumbName(v_HbaseImageThumbnail.getThumbName());
		return v_HbaseExifData;
	}

	protected HbaseExifDataOfImages buildHbaseExifDataOfImages(HbaseExifData v_hbaseImageThumbnail) {
		HbaseExifDataOfImages.Builder builder = HbaseExifDataOfImages.builder();
		builder.withCreationDate(DateTimeHelper.toDateTimeAsString(v_hbaseImageThumbnail.getCreationDate()))
				.withExifTag(v_hbaseImageThumbnail.getExifTag())
				.withExifValueAsByte(v_hbaseImageThumbnail.getExifValueAsByte())
				.withExifValueAsInt(v_hbaseImageThumbnail.getExifValueAsInt())
				.withExifValueAsShort(v_hbaseImageThumbnail.getExifValueAsShort())
				.withHeight(v_hbaseImageThumbnail.getHeight()).withImageId(v_hbaseImageThumbnail.getImageId())
				.withThumbName(v_hbaseImageThumbnail.getThumbName()).withWidth(v_hbaseImageThumbnail.getWidth());
		return builder.build();
	}

	protected HbaseExifData buildHbaseExifData(ExchangedTiffData v_hbaseImageThumbnail) {
		Builder builder = HbaseExifData.builder();
		builder.withExifValueAsByte(v_hbaseImageThumbnail.getDataAsByte())
				.withExifValueAsInt(v_hbaseImageThumbnail.getDataAsInt())
				.withExifValueAsShort(v_hbaseImageThumbnail.getDataAsShort())
				.withImageId(v_hbaseImageThumbnail.getImageId()).withExifTag(v_hbaseImageThumbnail.getTag());
		return builder.build();
	}

	private KStream<String, HbaseImageThumbnail> buildKStreamToGetHbaseImageThumbNail(StreamsBuilder builder) {
		KStream<String, HbaseImageThumbnail> stream = builder.stream(this.topicImageDataToPersist,
				Consumed.with(Serdes.String(),
						new HbaseImageThumbnailSerDe()));
		return stream;
	}

	private KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(StreamsBuilder streamsBuilder) {
		KStream<String, ExchangedTiffData> stream = streamsBuilder.stream(this.topicExif,
				Consumed.with(Serdes.String(),
						new ExchangedDataSerDe()));
		return stream;
	}

	protected void publishImageDataInRecordTopic(KStream<String, HbaseData> finalStream) {
		finalStream.to(this.topicExifImageDataToPersist,
				Produced.with(Serdes.String(),
						new HbaseDataSerDe()));
	}

}
