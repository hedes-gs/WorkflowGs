package com.workflow.model;

public class HbaseExifDateImage extends HbaseExif {

	private static final long serialVersionUID = 1L;

	//Number of nanosecondes since 1970
	protected long dateInNanosecondes ;

	public long getDateInNanosecondes() {
		return dateInNanosecondes;
	}

	public void setDateInNanosecondes(long dateInNanosecondes) {
		this.dateInNanosecondes = dateInNanosecondes;
	}

	public HbaseExifDateImage() {
	}
	
	
	
}
