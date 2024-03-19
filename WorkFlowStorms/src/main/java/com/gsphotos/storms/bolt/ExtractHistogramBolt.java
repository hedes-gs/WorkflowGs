package com.gsphotos.storms.bolt;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.builder.KeysBuilder.TopicThumbKey;
import com.workflow.model.storm.FinalImage;
import com.workflow.model.storm.ImageAndLut;

public class ExtractHistogramBolt extends BaseRichBolt {

    protected static final Logger LOGGER                 = LoggerFactory.getLogger(ExtractHistogramBolt.class);

    private static final String   IMAGE_AND_LUT          = "imageAndLut";
    private static final String   IMG_NUMBER             = "imgNumber";
    private static final String   VERSION_NUMBER         = "version";
    private static final String   FINAL_IMAGE_STREAM     = "finalImage";
    private static final String   NORMALIZE_IMAGE_STREAM = "normalizeImage";
    private static final String   IMAGE_KEY              = "imageKey";
    private static final int      RED                    = 0;
    private static final int      GREEN                  = 0;
    private static final int      BLUE                   = 0;
    /**
     *
     */
    private static final long     serialVersionUID       = 1L;

    private static final String   FINAL_IMAGE            = "finalImage";
    private OutputCollector       collector;

    private Map<String, Object>   stormConf;

    private TopologyContext       context;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.stormConf = stormConf;
        this.context = context;
        ExtractHistogramBolt.LOGGER.info("... Context " + context.toJSONString());
    }

    protected float[][] getHistogram(BufferedImage input) {
        // colorVal 1 -> RED 2 -> GREEN 3 -> BLUE
        float[][] histogram = new float[3][256];

        for (int i = 0; i < input.getWidth(); i++) {
            for (int j = 0; j < input.getHeight(); j++) {
                int red = 0;
                int rgb = input.getRGB(i, j);
                red = (rgb >> 16) & 0xFF;
                histogram[ExtractHistogramBolt.RED][red]++;
                red = (rgb >> 8) & 0xFF;
                histogram[ExtractHistogramBolt.GREEN][red]++;
                red = (rgb >> 0) & 0xFF;
                histogram[ExtractHistogramBolt.BLUE][red]++;
            }
        }
        return histogram;
    }

    @Override
    public void execute(Tuple input) {
        long time = System.currentTimeMillis();
        ExtractHistogramBolt.LOGGER.info("ExtractHistogramBolt : processing with msgId = {} ", input.getMessageId());
        try {
            this.doExecute(input);
            this.collector.ack(input);
        } catch (Exception e) {
            ExtractHistogramBolt.LOGGER.error("Unexpected error ", e);
            this.collector.fail(input);
        } finally {
            ExtractHistogramBolt.LOGGER.info(
                "ExtractHistogramBolt : end of processing time : {}",
                ((float) (System.currentTimeMillis() - time)) / 1000);
        }
    }

    protected void doExecute(Tuple input) {
        String id = (String) input.getValueByField("KEY");
        TopicThumbKey ttk = KeysBuilder.topicThumbKeyBuilder()
            .build(id);
        int imgCount = ttk.getImgNumber();
        String imgKey = ttk.getOriginalImageKey();

        byte[] data = (byte[]) input.getValueByField("VALUE");
        try {
            if ((data != null) && (data.length > 0)) {
                ExtractHistogramBolt.LOGGER
                    .info("[EVENT][{}] execute bolt ExtractHistogramBolt , length is {}", id, data.length);

                BufferedImage bi = ImageIO.read(new ByteArrayInputStream(data));
                float[][] histogram = this.getHistogram(bi);
                float[][] normaLizedHistogram = new float[histogram.length][];
                for (int i = 0; i < normaLizedHistogram.length; i++) {
                    normaLizedHistogram[i] = new float[histogram[i].length];
                    for (int k = 0; k < histogram[i].length; k++) {
                        normaLizedHistogram[i][k] = histogram[i][k];
                    }
                }
                // ===================== Normalizing Whole Image ========================
                this.normalizedFunction(
                    normaLizedHistogram[ExtractHistogramBolt.RED],
                    0,
                    normaLizedHistogram[0].length - 1);
                this.normalizedFunction(
                    normaLizedHistogram[ExtractHistogramBolt.GREEN],
                    0,
                    normaLizedHistogram[0].length - 1);
                this.normalizedFunction(
                    normaLizedHistogram[ExtractHistogramBolt.BLUE],
                    0,
                    normaLizedHistogram[0].length - 1);
                // ======================================================================

                // ===================== Histogram EQUALIZATION =========================
                this.histogramEqualization(normaLizedHistogram[0], 0, 255);
                this.histogramEqualization(normaLizedHistogram[1], 0, 255);
                this.histogramEqualization(normaLizedHistogram[2], 0, 255);
                // ======================================================================

                ArrayList<int[]> imageLUT = new ArrayList<int[]>();
                int[] rhistogram = new int[256];
                int[] ghistogram = new int[256];
                int[] bhistogram = new int[256];

                for (int i = 0; i < rhistogram.length; i++) {
                    rhistogram[i] = (int) normaLizedHistogram[ExtractHistogramBolt.RED][i];
                    ghistogram[i] = (int) normaLizedHistogram[ExtractHistogramBolt.GREEN][i];
                    bhistogram[i] = (int) normaLizedHistogram[ExtractHistogramBolt.BLUE][i];
                }
                imageLUT.add(rhistogram);
                imageLUT.add(ghistogram);
                imageLUT.add(bhistogram);
                ExtractHistogramBolt.LOGGER.info("[EVENT][{}] execute bolt ExtractHistogramBolt , emit 1", id);

                this.collector.emit(
                    ExtractHistogramBolt.NORMALIZE_IMAGE_STREAM,
                    input,
                    new Values(new ImageAndLut(id, data, imageLUT), (short) imgCount, imgKey));

                FinalImage.Builder builder = FinalImage.builder();
                builder.withDataId(id)
                    .withCompressedData(data);
                final FinalImage finalImage = builder.build();
                ExtractHistogramBolt.LOGGER.info("[EVENT][{}] execute bolt ExtractHistogramBolt , emit 2", id);
                this.collector.emit(
                    ExtractHistogramBolt.FINAL_IMAGE_STREAM,
                    input,
                    new Values(finalImage, (short) imgCount, imgKey));
                bi = null;
                ExtractHistogramBolt.LOGGER.info("[EVENT][{}] execute bolt ExtractHistogramBolt , emit done", id);

            } else {
                throw new IllegalArgumentException("Data is null...");
            }
        } catch (IOException e) {
            ExtractHistogramBolt.LOGGER.error("Unexpected error ", e);
            throw new IllegalArgumentException(e);
        }
    }

    protected void histogramEqualization(float histogram[], int low, int high) {

        float sumr, sumrx;
        sumr = sumrx = 0;
        int high_minus_low = high - low;
        for (int i = low; i <= high; i++) {
            sumr += (histogram[i]);
            sumrx = low + (high_minus_low * sumr);
            int valr = (int) (sumrx);
            if (valr > 255) {
                histogram[i] = 255;
            } else {
                histogram[i] = valr;
            }
        }
    }

    protected void normalizedFunction(float myArr[], int low, int high) {

        float sumV = 0.0f;
        for (int i = low; i <= high; i++) {
            sumV = sumV + (myArr[i]);
        }
        for (int i = low; i <= high; i++) {
            myArr[i] /= sumV;
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(
            new Fields(ExtractHistogramBolt.IMAGE_AND_LUT,
                ExtractHistogramBolt.FINAL_IMAGE,
                ExtractHistogramBolt.IMG_NUMBER,
                ExtractHistogramBolt.IMAGE_KEY));
        declarer.declareStream(
            ExtractHistogramBolt.NORMALIZE_IMAGE_STREAM,
            new Fields(ExtractHistogramBolt.IMAGE_AND_LUT,
                ExtractHistogramBolt.IMG_NUMBER,
                ExtractHistogramBolt.IMAGE_KEY));
        declarer.declareStream(
            ExtractHistogramBolt.FINAL_IMAGE_STREAM,
            new Fields(ExtractHistogramBolt.FINAL_IMAGE,
                ExtractHistogramBolt.VERSION_NUMBER,
                ExtractHistogramBolt.IMAGE_KEY));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return this.stormConf; }

}
