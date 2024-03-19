package com.gsphotos.imgops;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

public class HistogramEqualization {

	private final int greyRange = 256;

	public HistogramEqualization(String fileName) throws IOException {

		File file;
		BufferedImage image;
		float histogram[][];
		histogram = new float[3][];

		histogram[0] = new float[greyRange];
		histogram[1] = new float[greyRange];
		histogram[2] = new float[greyRange];

		file = new File(fileName);
		image = ImageIO.read(file);

		// ================== Histogram Generation ==============================
		histogram[0] = getHistogramByColor(image, 1);
		histogram[1] = getHistogramByColor(image, 2);
		histogram[2] = getHistogramByColor(image, 3);
		// ======================================================================

		// ===================== Normalizing Whole Image ========================
		normalizedFunction(histogram[0], 0, histogram[0].length - 1);
		normalizedFunction(histogram[1], 0, histogram[0].length - 1);
		normalizedFunction(histogram[2], 0, histogram[0].length - 1);
		// ======================================================================

		// ===================== Histogram EQUALIZATION =========================
		histogramEqualization(histogram[0], 0, 255);
		histogramEqualization(histogram[1], 0, 255);
		histogramEqualization(histogram[2], 0, 255);
		// ======================================================================

		ArrayList<int[]> imageLUT = new ArrayList<int[]>();
		int[] rhistogram = new int[256];
		int[] ghistogram = new int[256];
		int[] bhistogram = new int[256];

		for (int i = 0; i < rhistogram.length; i++) {
			rhistogram[i] = (int) histogram[0][i];
			ghistogram[i] = (int) histogram[1][i];
			bhistogram[i] = (int) histogram[2][i];
		}

		imageLUT.add(rhistogram);
		imageLUT.add(ghistogram);
		imageLUT.add(bhistogram);

		BufferedImage originalImage = getOriginalImage(ImageIO.read(file), imageLUT);
		String fileN = fileName(file);
		writeImage(file.getParent(), fileN, originalImage);
	}

	public void histogramEqualization(float histogram[], int low, int high) {

		float sumr, sumrx;
		sumr = sumrx = 0;
		for (int i = low; i <= high; i++) {
			sumr += (histogram[i]);
			sumrx = low + (high - low) * sumr;
			int valr = (int) (sumrx);
			if (valr > 255) {
				histogram[i] = 255;
			} else {
				histogram[i] = valr;
			}
		}
	}

	public BufferedImage getOriginalImage(BufferedImage original, ArrayList<int[]> histLUT) {

		int red;
		int green;
		int blue;
		int alpha;
		int newPixel = 0;

		// Get the Lookup table for histogram equalization

		BufferedImage histogramEQ = new BufferedImage(original.getWidth(), original.getHeight(), original.getType());

		for (int i = 0; i < original.getWidth(); i++) {
			for (int j = 0; j < original.getHeight(); j++) {

				// Get pixels by R, G, B
				alpha = new Color(original.getRGB(i, j)).getAlpha();
				red = new Color(original.getRGB(i, j)).getRed();
				green = new Color(original.getRGB(i, j)).getGreen();
				blue = new Color(original.getRGB(i, j)).getBlue();

				// Set new pixel values using the histogram lookup table
				red = histLUT.get(0)[red];
				green = histLUT.get(1)[green];
				blue = histLUT.get(2)[blue];

				// Return back to original format
				newPixel = colorToRGB(alpha, red, green, blue);

				// Write pixels into image
				histogramEQ.setRGB(i, j, newPixel);
			}
		}
		return histogramEQ;
	}

	// Convert R, G, B, Alpha to standard 8 bit
	public int colorToRGB(int alpha, int red, int green, int blue) {

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

	public void writeImage(String output, String fileName, BufferedImage img) throws IOException {
		File file = new File(output + "\\" + fileName + "HE.jpg");
		ImageIO.write(img, "jpg", file);
	}

	public String fileName(File file) throws IOException {
		String fileName = file.getName().substring(0, file.getName().length() - 4);
		return fileName;
	}

	public void normalizedFunction(float myArr[], int low, int high) {

		float sumV = 0.0f;
		for (int i = low; i <= high; i++) {
			sumV = sumV + (myArr[i]);
		}
		for (int i = low; i <= high; i++) {
			myArr[i] /= sumV;
		}
	}

	public float[] getHistogramByColor(BufferedImage input, int colorVal) {
		// colorVal 1 -> RED 2 -> GREEN 3 -> BLUE
		float[] histogram = new float[256];

		for (int i = 0; i < histogram.length; i++) {
			histogram[i] = 0.0f;
		}
		for (int i = 0; i < input.getWidth(); i++) {
			for (int j = 0; j < input.getHeight(); j++) {
				int red = 0;
				switch (colorVal) {
				case 1:
					red = new Color(input.getRGB(i, j)).getRed();
					break;
				case 2:
					red = new Color(input.getRGB(i, j)).getGreen();
					break;
				case 3:
					red = new Color(input.getRGB(i, j)).getBlue();
					break;
				}
				histogram[red]++;
			}
		}
		return histogram;
	}

	public static void main(String args[]) throws Exception {

		HistogramEqualization hist = new HistogramEqualization(
				"D:\\photos\\danses\\champ-latine-2018\\Output\\_HDE9078.jpg");
	}

	public HistogramEqualization() {
	}
}