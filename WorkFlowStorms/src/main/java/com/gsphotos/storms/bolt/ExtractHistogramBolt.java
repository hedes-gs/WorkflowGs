package com.gsphotos.storms.bolt;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.workflow.model.storm.FinalImage;
import com.workflow.model.storm.ImageAndLut;

public class ExtractHistogramBolt implements IRichBolt {

	private static final int RED = 0;
	private static final int GREEN = 0;
	private static final int BLUE = 0;
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	protected float[][] getHistogram(BufferedImage input) {
		// colorVal 1 -> RED 2 -> GREEN 3 -> BLUE
		float[][] histogram = new float[3][256];

		for (int i = 0; i < input.getWidth(); i++) {
			for (int j = 0; j < input.getHeight(); j++) {
				int red = 0;
				int rgb = input.getRGB(
					i,
					j);
				red = (rgb >> 16) & 0xFF;
				histogram[RED][red]++;
				red = (rgb >> 8) & 0xFF;
				histogram[GREEN][red]++;
				red = (rgb >> 0) & 0xFF;
				histogram[BLUE][red]++;
			}
		}
		return histogram;
	}

	@Override
	public void execute(Tuple input) {
		String id = (String) input.getValueByField(
			"KEY");
		byte[] data = (byte[]) input.getValueByField(
			"VALUE");
		try {
			if (data != null && data.length > 0) {
				BufferedImage bi = ImageIO.read(
					new ByteArrayInputStream(data));
				float[][] histogram = getHistogram(
					bi);
				float[][] normaLizedHistogram = new float[histogram.length][];
				for (int i = 0; i < normaLizedHistogram.length; i++) {
					normaLizedHistogram[i] = new float[histogram[i].length];
					for (int k = 0; k < histogram[i].length; k++) {
						normaLizedHistogram[i][k] = histogram[i][k];
					}
				}
				// ===================== Normalizing Whole Image ========================
				normalizedFunction(
					normaLizedHistogram[RED],
					0,
					normaLizedHistogram[0].length - 1);
				normalizedFunction(
					normaLizedHistogram[GREEN],
					0,
					normaLizedHistogram[0].length - 1);
				normalizedFunction(
					normaLizedHistogram[BLUE],
					0,
					normaLizedHistogram[0].length - 1);
				// ======================================================================

				// ===================== Histogram EQUALIZATION =========================
				histogramEqualization(
					normaLizedHistogram[0],
					0,
					255);
				histogramEqualization(
					normaLizedHistogram[1],
					0,
					255);
				histogramEqualization(
					normaLizedHistogram[2],
					0,
					255);
				// ======================================================================

				ArrayList<int[]> imageLUT = new ArrayList<int[]>();
				int[] rhistogram = new int[256];
				int[] ghistogram = new int[256];
				int[] bhistogram = new int[256];

				for (int i = 0; i < rhistogram.length; i++) {
					rhistogram[i] = (int) normaLizedHistogram[RED][i];
					ghistogram[i] = (int) normaLizedHistogram[GREEN][i];
					bhistogram[i] = (int) normaLizedHistogram[BLUE][i];
				}
				imageLUT.add(
					rhistogram);
				imageLUT.add(
					ghistogram);
				imageLUT.add(
					bhistogram);

				collector.emit(
					"normalizeImage",
					input,
					new Values(new ImageAndLut(id, data, imageLUT)));
				collector.emit(
					"originalImage",
					input,
					new Values(new FinalImage(id, true, 0, 0, data), Boolean.FALSE));
				bi = null;
			} else {
				collector.fail(
					input);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void histogramEqualization(float histogram[], int low, int high) {

		float sumr, sumrx;
		sumr = sumrx = 0;
		int high_minus_low = high - low;
		for (int i = low; i <= high; i++) {
			sumr += (histogram[i]);
			sumrx = low + high_minus_low * sumr;
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
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
			new Fields("imageAndLut", "originalImage", "isNormalized"));
		declarer.declareStream(
			"normalizeImage",
			new Fields("imageAndLut"));
		declarer.declareStream(
			"originalImage",
			new Fields("originalImage", "isNormalized"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
