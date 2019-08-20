package com.gsphotos.workflow.poll.model;

import org.springframework.data.redis.core.RedisHash;

@RedisHash("ImageInformation")
public class ImageInformation {

	protected String imageName;
	protected String pathName;
	protected String creationDate;

	static {

	}
}
