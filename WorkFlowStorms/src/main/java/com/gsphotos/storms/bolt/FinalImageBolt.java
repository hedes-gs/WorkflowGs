package com.gsphotos.storms.bolt;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.gs.photos.serializers.FinalImageSerializer;
import com.workflow.model.storm.FinalImage;

public class FinalImageBolt implements IRichBolt {

	protected static final Logger LOGGER = Logger.getLogger(
		FinalImageBolt.class);

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
			return width;
		}

		public short getHeight() {
			return height;
		}

		@Override
		public String toString() {
			return "Dim [width=" + width + ", height=" + height + "]";
		}

	}

	private static final long serialVersionUID = 1;
	private OutputCollector collector;
	protected Properties settings = new Properties();
	protected Producer<String, FinalImage> producer;
	protected String kafkaBrokers;
	protected String outputTopic;

	protected Dim get(byte[] jpeg_thumbnail) {
		ByteBuffer buffer = ByteBuffer.wrap(
			jpeg_thumbnail);
		short imgHeight = 0;
		short imgWidth = 0;
		short SOIThumbnail = buffer.getShort();
		if (SOIThumbnail == (short) 0xffd8) {
			boolean finished = false;
			boolean found = false;
			while (!finished) {
				short marker = buffer.getShort();
				found = marker == (short) 0xffc0;
				finished = found || buffer.position() >= jpeg_thumbnail.length;
				if (!finished) {
					short lengthOfMarker = buffer.getShort();
					buffer.position(
						buffer.position() + lengthOfMarker - 2);
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

		settings.put(
			StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
			kafkaBrokers);
		settings.put(
			"key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(
			"value.serializer",
			FinalImageSerializer.class.getName());
		producer = new KafkaProducer<>(settings);
	}

	@Override
	public void execute(Tuple input) {
		FinalImage finalImage = null;

		FinalImage currentImage = (FinalImage) input.getValueByField(
			"originalImage");
		boolean isNormalized = input.getBooleanByField(
			"isNormalized");
		Dim dim = get(
			currentImage.getCompressedImage());

		finalImage = new FinalImage(
			currentImage.getId(),
			isNormalized,
			dim.getWidth(),
			dim.getHeight(),
			currentImage.getCompressedImage());
		LOGGER.info(
			" receiving final image input " + finalImage);

		producer.send(
			new ProducerRecord<String, FinalImage>(outputTopic, finalImage.getId(), finalImage));
		collector.ack(
			input);
	};

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public FinalImageBolt(
			String kafkaBrokers,
			String outputTopic) {
		super();
		this.kafkaBrokers = kafkaBrokers;
		this.outputTopic = outputTopic;
	}

}
