package com.workflow.model.storm;

import java.io.Serializable;

public class ImageAndHistogram implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	protected String id;
	protected float[][] histogramm;

	public String getId() {
		return id;
	}

	public float[][] getHistogramm() {
		return histogramm;
	}

	public ImageAndHistogram(String id, float[][] histogramm) {
		super();
		this.id = id;
		this.histogramm = histogramm;
	}

	public ImageAndHistogram() {
	}

}
