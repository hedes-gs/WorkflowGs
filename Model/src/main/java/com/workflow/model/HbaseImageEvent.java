package com.workflow.model;

import java.io.Serializable;

public class HbaseImageEvent implements Serializable {

	private static final long serialVersionUID = 1L;
	protected String imageId;
	protected long creationDate;
	protected String path;
	protected String imageName;
	protected String thumbName;

	protected String eventName;
	protected String dateOfEvent;
	protected String eventClient;

	public String getImageId() {
		return imageId;
	}

	public void setImageId(String imageId) {
		this.imageId = imageId;
	}

	public long getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getImageName() {
		return imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public String getThumbName() {
		return thumbName;
	}

	public void setThumbName(String thumbName) {
		this.thumbName = thumbName;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public String getEventClient() {
		return eventClient;
	}

	public void setEventClient(String eventClient) {
		this.eventClient = eventClient;
	}

}
