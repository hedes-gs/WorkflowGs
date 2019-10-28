package com.gs.photo.workflow.impl;

import java.nio.ByteBuffer;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IgniteDAO implements IIgniteDAO {

	protected final Logger                LOGGER = LoggerFactory.getLogger(IgniteDAO.class);

	@Autowired
	protected IgniteCache<String, byte[]> igniteCache;

	@Override
	public void save(String key, ByteBuffer rawFile) {
		boolean saved = this.igniteCache.putIfAbsent(key,
				rawFile.array());
		if (!saved) {
			this.LOGGER.warn("Warning : file with key {} already exist",
					key);
		}

	}

}
