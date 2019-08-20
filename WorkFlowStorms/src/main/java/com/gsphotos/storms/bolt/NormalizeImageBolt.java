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
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.workflow.model.storm.ImageAndLut;
import com.workflow.model.storm.NormalizedImage;

public class NormalizeImageBolt implements IRichBolt {

	private static final long serialVersionUID = 1;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		ImageAndLut data = (ImageAndLut) input.getValueByField(
			"imageAndLut");
		System.err.println(
			".... NormalizeImageBolt : receiving " + data.getId());

		try {
			BufferedImage bi = ImageIO.read(
				new ByteArrayInputStream(data.getCompressedImage()));
			BufferedImage mmodifiedImage = getOriginalImage(
				bi,
				data.getLut());
			ByteArrayOutputStream os = new ByteArrayOutputStream(16384);
			ImageIO.write(
				mmodifiedImage,
				"jpg",
				os);
			collector.emit(
				"finalImage",
				new Values(new NormalizedImage(data.getId(), os.toByteArray()), Boolean.TRUE));
			collector.ack(
				input);
		} catch (IOException e) {
			e.printStackTrace();
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

		int[] redLut = histLUT.get(
			0);
		int[] greenLut = histLUT.get(
			1);
		int[] blueLut = histLUT.get(
			2);
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {

				// Get pixels by R, G, B
				int rgb = original.getRGB(
					i,
					j);
				alpha = (rgb >> 24) & 0xff;
				red = (rgb >> 16) & 0xff;
				green = (rgb >> 8) & 0xff;
				blue = (rgb >> 0) & 0xff;

				// Set new pixel values using the histogram lookup table
				red = redLut[red];
				green = greenLut[green];
				blue = blueLut[blue];

				// Return back to original format
				newPixel = colorToRGB(
					alpha,
					red,
					green,
					blue);

				// Write pixels into image
				histogramEQ.setRGB(
					i,
					j,
					newPixel);
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
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
			new Fields("finalImage", "isNormalized"));
		declarer.declareStream(
			"finalImage",
			new Fields("finalImage", "isNormalized"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
