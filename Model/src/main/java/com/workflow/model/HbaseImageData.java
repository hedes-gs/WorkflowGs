package com.workflow.model;

import java.io.Serializable;

public class HbaseImageData implements Serializable{

	private static final long serialVersionUID = 1L;

	protected String imageId ;
	protected String path ;
	protected String name ;
	protected long   creationDate ;
	
	public String getImageId() {
		return imageId;
	}
	public void setImageId(String imageId) {
		this.imageId = imageId;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
	public HbaseImageData() {
		
	}
	
	
	
	
}
