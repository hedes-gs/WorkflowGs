package com.gsphotos.storms;

import javax.annotation.PostConstruct;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import com.gsphotos.storms.bolt.ExtractHistogramBolt;
import com.gsphotos.storms.bolt.FinalImageBolt;
import com.gsphotos.storms.bolt.NormalizeImageBolt;

public class StormTopology implements IStormTopology {

    private static final String INPUT_TOPIC                      = "thumb";
    private static final String STORM_TOPOLY_NAME                = "KafkaStormSample";
    private static final String STORM_STREAM_ORIGINAL_IMAGE      = "originalImage";
    private static final String STORM_STREAM_NORMALIZE_IMAGE     = "normalizeImage";
    private static final String COMPONENT_FINAL_IMAGE_BOLT       = "final-image-bolt";
    private static final String COMPONENT_NORMALIZE_IMAGE_BOLT   = "normalize-image-bolt";
    private static final String COMPONENT_EXTRACT_HISTOGRAM_BOLT = "extract-histogram-bolt";
    private static final String COMPONENT_KAFKA_SPOUT_NAME       = "kafka-spout";

    protected String            zookeeperConnect;

    protected int               kafkaSpoutConfigBufferSizeBytes;

    protected int               kafkaSpoutConfigFetchSizeBytes;

    protected String            kafkaInputTopic;

    protected String            kafkaOutputTopic;

    protected String            topicEvent;

    protected String            kafkaBrokers;

    @PostConstruct
    public void build() {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        config.put(Config.TOPOLOGY_DEBUG, false);

        String zkConnString = this.zookeeperConnect;
        String topic = this.kafkaInputTopic;
        // BrokerHosts hosts = new ZkHosts(zkConnString);

        KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig(null);
        /*
         * kafkaSpoutConfig.bufferSizeBytes = kafkaSpoutConfigBufferSizeBytes;
         * kafkaSpoutConfig.fetchSizeBytes = kafkaSpoutConfigFetchSizeBytes;
         * kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
         *
         * kafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new
         * KeyValueScheme() {
         *
         * @Override public Fields getOutputFields() { return new Fields("KEY",
         * "VALUE"); }
         *
         * @Override public List<Object> deserialize(ByteBuffer ser) { return null; }
         *
         * @Override public List<Object> deserializeKeyAndValue(ByteBuffer key,
         * ByteBuffer value) { String keyString = StringScheme.deserializeString( key);
         * byte[] valueAsByte = Utils.toArray( value); return new Values(
         * ImmutableMap.of( keyString, valueAsByte)); } });
         */

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(StormTopology.COMPONENT_KAFKA_SPOUT_NAME, new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt(StormTopology.COMPONENT_EXTRACT_HISTOGRAM_BOLT, new ExtractHistogramBolt())
            .shuffleGrouping(StormTopology.COMPONENT_KAFKA_SPOUT_NAME);
        builder.setBolt(StormTopology.COMPONENT_NORMALIZE_IMAGE_BOLT, new NormalizeImageBolt())
            .shuffleGrouping(
                StormTopology.COMPONENT_EXTRACT_HISTOGRAM_BOLT,
                StormTopology.STORM_STREAM_NORMALIZE_IMAGE);
        builder
            .setBolt(
                StormTopology.COMPONENT_FINAL_IMAGE_BOLT,
                new FinalImageBolt(this.kafkaBrokers, this.kafkaOutputTopic, this.topicEvent))
            .shuffleGrouping(StormTopology.COMPONENT_NORMALIZE_IMAGE_BOLT, "finalImage")
            .shuffleGrouping(StormTopology.COMPONENT_EXTRACT_HISTOGRAM_BOLT, StormTopology.STORM_STREAM_ORIGINAL_IMAGE);
        try {
            StormSubmitter.submitTopologyWithProgressBar("gs-topo", config, builder.createTopology());
        } catch (
            AlreadyAliveException |
            InvalidTopologyException |
            AuthorizationException e) {
            e.printStackTrace();
        }
        /*
         * LocalCluster cluster = new LocalCluster(); cluster.submitTopology(
         * STORM_TOPOLY_NAME, config, builder.createTopology());
         */

    }

}