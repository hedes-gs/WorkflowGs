package com.workflow.model.dtos;

import javax.xml.bind.annotation.XmlType;

@XmlType
public class ImageMetaData {

	protected String id;
	protected String key;
	protected String value;

	public ImageMetaData(
		String id,
		String key,
		String value) {
		super();
		this.id = id;
		this.key = key;
		this.value = value;
	}

	public ImageMetaData() {
	}

}
