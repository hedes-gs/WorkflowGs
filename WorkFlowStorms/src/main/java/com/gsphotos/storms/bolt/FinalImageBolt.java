package com.gsphotos.storms.bolt;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import com.gs.photos.serializers.FinalImageSerializer;
import com.workflow.model.storm.FinalImage;

public class FinalImageBolt extends BaseWindowedBolt {

	protected Map<String, Object> windowConfiguration;

	private static final String   IS_NORMALIZED  = "isNormalized";
	private static final String   ORIGINAL_IMAGE = "originalImage";
	protected static final Logger LOGGER         = Logger.getLogger(FinalImageBolt.class);

	public static class Dim {
		protected final short width;
		protected final short height;

		public Dim(
				short width,
				short height) {
			super();
			this.width = width;
			this.height = height;
		}

		public short getWidth() {
			return this.width;
		}

		public short getHeight() {
			return this.height;
		}

		@Override
		public String toString() {
			return "Dim [width=" + this.width + ", height=" + this.height + "]";
		}

	}

	private static final long              serialVersionUID = 1;
	private OutputCollector                collector;
	protected Properties                   settings         = new Properties();
	protected Producer<String, FinalImage> producer;
	protected String                       kafkaBrokers;
	protected String                       outputTopic;
	protected int                          windowLength;

	protected Dim get(byte[] jpeg_thumbnail) {
		ByteBuffer buffer = ByteBuffer.wrap(jpeg_thumbnail);
		short imgHeight = 0;
		short imgWidth = 0;
		short SOIThumbnail = buffer.getShort();
		if (SOIThumbnail == (short) 0xffd8) {
			boolean finished = false;
			boolean found = false;
			while (!finished) {
				short marker = buffer.getShort();
				found = marker == (short) 0xffc0;
				finished = found || (buffer.position() >= jpeg_thumbnail.length);
				if (!finished) {
					short lengthOfMarker = buffer.getShort();
					buffer.position((buffer.position() + lengthOfMarker) - 2);
				}
			}
			if (found) {
				short lengthOfMarker = buffer.getShort();
				byte dataPrecision = buffer.get();
				imgHeight = buffer.getShort();
				imgWidth = buffer.getShort();
			}
		}
		buffer.clear();
		return new Dim(imgWidth, imgHeight);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		this.settings.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				this.kafkaBrokers);
		this.settings.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		this.settings.put("value.serializer",
				FinalImageSerializer.class.getName());
		this.producer = new KafkaProducer<>(this.settings);
		this.windowConfiguration = new HashMap<>();
		this.buildComponentConfiguration();

	}

	protected void buildComponentConfiguration() {
		this.windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT,
				this.windowLength);
		this.windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT,
				1);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> retValue = super.getComponentConfiguration();
		if (retValue == null) {
			retValue = new HashMap<String, Object>();

		}
		this.windowConfiguration = retValue;
		this.buildComponentConfiguration();
		return this.windowConfiguration;
	}

	public FinalImageBolt(
			String kafkaBrokers,
			String outputTopic) {
		super();
		this.kafkaBrokers = kafkaBrokers;
		this.outputTopic = outputTopic;
	}

	public FinalImageBolt() {
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		inputWindow.get().forEach((input) -> {
			FinalImage finalImage = null;

			FinalImage currentImage = (FinalImage) input.getValueByField(FinalImageBolt.ORIGINAL_IMAGE);
			boolean isNormalized = input.getBooleanByField(FinalImageBolt.IS_NORMALIZED);
			Dim dim = this.get(currentImage.getCompressedImage());
			FinalImage.Builder builder = FinalImage.builder();
			builder.withCompressedData(currentImage.getCompressedImage()).withHeight(dim.getHeight())
					.withWidth(dim.getWidth()).withOriginal(isNormalized).withId(currentImage.getId());
			finalImage = builder.build();
			FinalImageBolt.LOGGER.info(" sending final image input " + finalImage);
			this.producer
					.send(new ProducerRecord<String, FinalImage>(this.outputTopic, finalImage.getId(), finalImage));

		});
		this.producer.flush();
		inputWindow.get().forEach((input) -> {
			this.collector.ack(input);
		});
	}

	public String getKafkaBrokers() {
		return this.kafkaBrokers;
	}

	public void setKafkaBrokers(String kafkaBrokers) {
		this.kafkaBrokers = kafkaBrokers;
	}

	public String getOutputTopic() {
		return this.outputTopic;
	}

	public void setOutputTopic(String outputTopic) {
		this.outputTopic = outputTopic;
	}

	public int getWindowLength() {
		return this.windowLength;
	}

	public void setWindowLength(int windowLength) {
		this.windowLength = windowLength;
	}

}
