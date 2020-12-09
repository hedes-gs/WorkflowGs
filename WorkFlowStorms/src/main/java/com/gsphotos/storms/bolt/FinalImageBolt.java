package com.gsphotos.storms.bolt;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.gs.photos.serializers.FinalImageSerializer;
import com.gs.photos.serializers.WfEventsSerializer;
import com.workflow.model.builder.KeysBuilder.FinalImageKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.storm.FinalImage;

public class FinalImageBolt extends BaseRichBolt {
    protected static final Logger LOGGER      = LoggerFactory.getLogger(FinalImageBolt.class);

    private static final String   VERSION     = "version";
    private static String         FINAL_IMAGE = "finalImage";
    private static final String   IMAGE_KEY   = "imageKey";

    public static class Dim {
        protected final short width;
        protected final short height;

        public Dim(
            short width,
            short height
        ) {
            super();
            this.width = width;
            this.height = height;
        }

        public short getWidth() { return this.width; }

        public short getHeight() { return this.height; }

        @Override
        public String toString() { return "Dim [width=" + this.width + ", height=" + this.height + "]"; }

    }

    private static final long            serialVersionUID     = 1;
    private OutputCollector              collector;
    protected Properties                 settingForFinalImage = new Properties();
    protected Producer<String, Object>   producerOfFinalImage;
    protected Properties                 settingForWfEvents   = new Properties();
    protected Producer<String, WfEvents> producerOfEvents;
    protected String                     kafkaBrokers;
    protected String                     outputTopic;
    protected String                     eventTopic;
    protected int                        windowLength;
    protected int                        windowDuration;
    protected Map<String, Object>        windowConfiguration;

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
        this.windowConfiguration = new HashMap<>();

        this.settingForFinalImage.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBrokers);
        this.settingForFinalImage
            .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.settingForFinalImage
            .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FinalImageSerializer.class.getName());
        this.settingForFinalImage.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        this.settingForFinalImage.put("sasl.kerberos.service.name", "kafka");
        this.settingForFinalImage.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 4194304);
        this.producerOfFinalImage = new KafkaProducer<>(this.settingForFinalImage);

        this.settingForWfEvents.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBrokers);
        this.settingForWfEvents
            .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.settingForWfEvents.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WfEventsSerializer.class.getName());
        this.settingForWfEvents.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        this.settingForWfEvents.put("sasl.kerberos.service.name", "kafka");
        this.settingForWfEvents.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 4194304);
        this.producerOfEvents = new KafkaProducer<>(this.settingForWfEvents);
        this.buildComponentConfiguration();
    }

    protected void buildComponentConfiguration() {
        /*
         * this.windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,
         * this.windowDuration);
         */

    }

    public FinalImageBolt(
        String kafkaBrokers,
        String outputTopic,
        String eventTopic
    ) {
        super();
        this.windowConfiguration = new HashMap<>();
        this.buildComponentConfiguration();
        this.kafkaBrokers = kafkaBrokers;
        this.outputTopic = outputTopic;
        this.eventTopic = eventTopic;
    }

    public FinalImageBolt() {}

    @Override
    public void execute(Tuple inputWindow) {
        long time = System.currentTimeMillis();
        List<Tuple> tuples = Arrays.asList(inputWindow);
        FinalImageBolt.LOGGER.info(
            "FinalImageBolt : Processing window tuple from source-streamId = {} , source-component = {}, anchor = {}",
            inputWindow.getSourceStreamId(),
            inputWindow.getSourceComponent(),
            inputWindow.getMessageId()
                .getAnchorsToIds());
        try {
            this.doExecute(tuples);
            this.collector.ack(inputWindow);
        } catch (Exception e) {
            FinalImageBolt.LOGGER.error("Unexpected error", e);
        } finally {
            FinalImageBolt.LOGGER.info(
                "FinalImageBolt : End of processing window tuple with size {},  duration : {}",
                tuples.size(),
                ((float) (System.currentTimeMillis() - time)) / 1000);
        }
    }

    protected void doExecute(List<Tuple> tuples) {
        Multimap<String, WfEvent> multiMapOfEvents = ArrayListMultimap.create();
        tuples.forEach((input) -> {

            FinalImage currentImage = (FinalImage) input.getValueByField(FinalImageBolt.FINAL_IMAGE);
            FinalImageBolt.LOGGER.info("[EVENT][{}] receive one bolt FinalImageBolt", currentImage.getDataId());

            short version = input.getShortByField(FinalImageBolt.VERSION);
            String imgKey = input.getStringByField(FinalImageBolt.IMAGE_KEY);
            Dim dim = this.get(currentImage.getCompressedImage());
            FinalImage.Builder builder = FinalImage.builder();
            builder.withCompressedData(currentImage.getCompressedImage())
                .withHeight(dim.getHeight())
                .withWidth(dim.getWidth())
                .withVersion(version)
                .withDataId(currentImage.getDataId());
            FinalImage finalImage = builder.build();

            this.producerOfFinalImage.send(
                new ProducerRecord<String, Object>(this.outputTopic, imgKey, finalImage),
                (a, e) -> this.process(finalImage.getDataId() + "-" + version, a, e));
            final WfEvent wfEvent = this.buildEvent(imgKey, finalImage, Short.toString(version));
            multiMapOfEvents.put(imgKey, wfEvent);
            FinalImageBolt.LOGGER
                .info("[EVENT][{}] Produce finalImage, version is {}", finalImage.getDataId(), finalImage.getVersion());
        });
        multiMapOfEvents.keySet()
            .forEach(
                (k) -> this.producerOfEvents.send(
                    new ProducerRecord<String, WfEvents>(this.eventTopic,
                        k,
                        WfEvents.builder()
                            .withDataId("<unset>")
                            .withProducer("STORM_BUILD_FINAL_IMAGE")
                            .withEvents(multiMapOfEvents.get(k))
                            .build())));
        this.producerOfFinalImage.flush();
        this.producerOfEvents.flush();
    }

    private WfEvent buildEvent(String imgKey, FinalImage finalImage, String version) {
        String hbdHashCode = FinalImageKeyBuilder.build(finalImage, version);
        return WfEventRecorded.builder()
            .withImgId(imgKey)
            .withParentDataId(finalImage.getDataId())
            .withDataId(hbdHashCode)
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_IMG_PROCESSOR)
            .build();
    }

    private void process(String imgId, RecordMetadata a, Exception e) {
        if (e != null) {
            FinalImageBolt.LOGGER
                .warn("[EVENT][{}] finalImage not sent due to the error", imgId, ExceptionUtils.getStackFrames(e));
        } else {
            FinalImageBolt.LOGGER.info("[EVENT][{}] finalImage, sent at offset {}", imgId, a.toString());
        }
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEventProduced.builder()
            .withDataId(dataId)
            .withParentDataId(parentDataId)
            .withImgId(imageKey)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_IMG_PROCESSOR)
                    .build())
            .build();
    }

    public String getKafkaBrokers() { return this.kafkaBrokers; }

    public void setKafkaBrokers(String kafkaBrokers) { this.kafkaBrokers = kafkaBrokers; }

    public String getOutputTopic() { return this.outputTopic; }

    public void setOutputTopic(String outputTopic) { this.outputTopic = outputTopic; }

    public String getEventTopic() { return this.eventTopic; }

    public void setEventTopic(String eventTopic) { this.eventTopic = eventTopic; }

    public int getWindowLength() { return this.windowLength; }

    public void setWindowLength(int windowLength) { this.windowLength = windowLength; }

    public int getWindowDuration() { return this.windowDuration; }

    public void setWindowDuration(int windowDuration) { this.windowDuration = windowDuration; }

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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
