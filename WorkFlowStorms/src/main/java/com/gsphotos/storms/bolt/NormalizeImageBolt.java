package com.gsphotos.storms.bolt;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
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

import com.workflow.model.storm.FinalImage;
import com.workflow.model.storm.ImageAndLut;

public class NormalizeImageBolt extends BaseRichBolt {
    protected static final Logger LOGGER             = LoggerFactory.getLogger(ExtractHistogramBolt.class);

    private static final String   FINAL_IMAGE_STREAM = "finalImage";
    private static final String   VERSION_NUMBER     = "version";
    private static final String   IMAGE_AND_LUT      = "imageAndLut";
    private static final String   IMG_NUMBER         = "imgNumber";
    private static final String   IMAGE_KEY          = "imageKey";

    private static final long     serialVersionUID   = 1;
    private OutputCollector       collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        long time = System.currentTimeMillis();
        NormalizeImageBolt.LOGGER.info("NormalizeImageBolt : processing");
        try {
            this.doExecute(input);
            this.collector.ack(input);
        } catch (Exception e) {
            NormalizeImageBolt.LOGGER.error("Error ", e);
            this.collector.fail(input);
        } finally {
            NormalizeImageBolt.LOGGER.info(
                "NormalizeImageBolt : end of processing time : {}",
                ((float) (System.currentTimeMillis() - time)) / 1000);
        }
    }

    protected void doExecute(Tuple input) {
        ImageAndLut data = (ImageAndLut) input.getValueByField(NormalizeImageBolt.IMAGE_AND_LUT);
        String imgKey = input.getStringByField(NormalizeImageBolt.IMAGE_KEY);

        ExtractHistogramBolt.LOGGER.info("[EVENT][{}] execute bolt NormalizeImageBolt , receive data", data.getId());
        short imgNumber = input.getShortByField(NormalizeImageBolt.IMG_NUMBER);
        try {
            BufferedImage bi = ImageIO.read(new ByteArrayInputStream(data.getCompressedImage()));
            BufferedImage mmodifiedImage = this.getOriginalImage(bi, data.getLut());
            ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
            ImageIO.write(mmodifiedImage, "jpg", os);
            FinalImage.Builder builder = FinalImage.builder();
            builder.withDataId(data.getId())
                .withCompressedData(os.toByteArray());

            this.collector.emit(
                NormalizeImageBolt.FINAL_IMAGE_STREAM,
                input,
                new Values(builder.build(), (short) (100 * imgNumber), imgKey));
            ExtractHistogramBolt.LOGGER.info("[EVENT][{}] execute bolt NormalizeImageBolt , emit data", data.getId());
        } catch (IOException e) {
            NormalizeImageBolt.LOGGER.error("Error ", e);
        }

    }

    protected BufferedImage getOriginalImage(BufferedImage original, List<int[]> histLUT) {

        int red;
        int green;
        int blue;
        int alpha;
        int newPixel = 0;

        // Get the Lookup table for histogram equalization

        int width = original.getWidth();
        int height = original.getHeight();
        BufferedImage histogramEQ = new BufferedImage(width, height, original.getType());

        int[] redLut = histLUT.get(0);
        int[] greenLut = histLUT.get(1);
        int[] blueLut = histLUT.get(2);
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {

                // Get pixels by R, G, B
                int rgb = original.getRGB(i, j);
                alpha = (rgb >> 24) & 0xff;
                red = (rgb >> 16) & 0xff;
                green = (rgb >> 8) & 0xff;
                blue = (rgb >> 0) & 0xff;

                // Set new pixel values using the histogram lookup table
                red = redLut[red];
                green = greenLut[green];
                blue = blueLut[blue];

                // Return back to original format
                newPixel = this.colorToRGB(alpha, red, green, blue);

                // Write pixels into image
                histogramEQ.setRGB(i, j, newPixel);
            }
        }
        return histogramEQ;
    }

    // Convert R, G, B, Alpha to standard 8 bit
    private int colorToRGB(int alpha, int red, int green, int blue) {

        int newPixel = 0;
        newPixel += alpha;
        newPixel = newPixel << 8;
        newPixel += red;
        newPixel = newPixel << 8;
        newPixel += green;
        newPixel = newPixel << 8;
        newPixel += blue;
        return newPixel;

    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(
            new Fields(NormalizeImageBolt.FINAL_IMAGE_STREAM,
                NormalizeImageBolt.VERSION_NUMBER,
                NormalizeImageBolt.IMAGE_KEY));
        declarer.declareStream(
            NormalizeImageBolt.FINAL_IMAGE_STREAM,
            new Fields(NormalizeImageBolt.FINAL_IMAGE_STREAM,
                NormalizeImageBolt.VERSION_NUMBER,
                NormalizeImageBolt.IMAGE_KEY));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }

}
