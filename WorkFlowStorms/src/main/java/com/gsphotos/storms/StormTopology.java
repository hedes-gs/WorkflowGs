package com.gsphotos.storms;

import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kafka.common.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;
import com.gsphotos.storms.bolt.ExtractHistogramBolt;
import com.gsphotos.storms.bolt.FinalImageBolt;
import com.gsphotos.storms.bolt.NormalizeImageBolt;

public class StormTopology implements IStormTopology {

	private static final String INPUT_TOPIC = "thumb";
	private static final String STORM_TOPOLY_NAME = "KafkaStormSample";
	private static final String STORM_STREAM_ORIGINAL_IMAGE = "originalImage";
	private static final String STORM_STREAM_NORMALIZE_IMAGE = "normalizeImage";
	private static final String COMPONENT_FINAL_IMAGE_BOLT = "final-image-bolt";
	private static final String COMPONENT_NORMALIZE_IMAGE_BOLT = "normalize-image-bolt";
	private static final String COMPONENT_EXTRACT_HISTOGRAM_BOLT = "extract-histogram-bolt";
	private static final String COMPONENT_KAFKA_SPOUT_NAME = "kafka-spout";

	protected String zookeeperConnect;

	protected int kafkaSpoutConfigBufferSizeBytes;

	protected int kafkaSpoutConfigFetchSizeBytes;

	protected String kafkaInputTopic;

	protected String kafkaOutputTopic;

	protected String kafkaBrokers;

	@PostConstruct
	public void build() {
		Config config = new Config();
		config.setDebug(
			true);
		config.put(
			Config.TOPOLOGY_MAX_SPOUT_PENDING,
			1);
		config.put(
			Config.TOPOLOGY_DEBUG,
			false);

		String zkConnString = zookeeperConnect;
		String topic = kafkaInputTopic;
		BrokerHosts hosts = new ZkHosts(zkConnString);

		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "storm-client", "/" + topic, "kafka-storm-spout");
		kafkaSpoutConfig.bufferSizeBytes = kafkaSpoutConfigBufferSizeBytes;
		kafkaSpoutConfig.fetchSizeBytes = kafkaSpoutConfigFetchSizeBytes;
		kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		kafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new KeyValueScheme() {

			@Override
			public Fields getOutputFields() {
				return new Fields("KEY", "VALUE");
			}

			@Override
			public List<Object> deserialize(ByteBuffer ser) {
				return null;
			}

			@Override
			public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
				String keyString = StringScheme.deserializeString(
					key);
				byte[] valueAsByte = Utils.toArray(
					value);
				return new Values(
					ImmutableMap.of(
						keyString,
						valueAsByte));
			}
		});

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(
			COMPONENT_KAFKA_SPOUT_NAME,
			new KafkaSpout(kafkaSpoutConfig));
		builder.setBolt(
			COMPONENT_EXTRACT_HISTOGRAM_BOLT,
			new ExtractHistogramBolt()).shuffleGrouping(
				COMPONENT_KAFKA_SPOUT_NAME);
		builder.setBolt(
			COMPONENT_NORMALIZE_IMAGE_BOLT,
			new NormalizeImageBolt()).shuffleGrouping(
				COMPONENT_EXTRACT_HISTOGRAM_BOLT,
				STORM_STREAM_NORMALIZE_IMAGE);
		builder.setBolt(
			COMPONENT_FINAL_IMAGE_BOLT,
			new FinalImageBolt(kafkaBrokers, kafkaOutputTopic)).shuffleGrouping(
				COMPONENT_NORMALIZE_IMAGE_BOLT,
				"finalImage").shuffleGrouping(
					COMPONENT_EXTRACT_HISTOGRAM_BOLT,
					STORM_STREAM_ORIGINAL_IMAGE);
		try {
			StormSubmitter.submitTopologyWithProgressBar(
				"gs-topo",
				config,
				builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
		/*
		 * LocalCluster cluster = new LocalCluster(); cluster.submitTopology(
		 * STORM_TOPOLY_NAME, config, builder.createTopology());
		 */

	}

}